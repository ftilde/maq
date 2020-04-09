use crate::{Backend, Matcher};
use core::cell::Cell;
use core::pin::Pin;
use core::sync::atomic::AtomicU64;
use core::task::Context;
use core::task::Poll;
use core::task::{RawWaker, RawWakerVTable, Waker};
use io_uring::opcode::{types::Target, Close, Openat, Read};
use io_uring::squeue::Entry;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::future::Future;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

pub struct IoUringBackend;

const SIZE_POW: usize = 5;

impl IoUringBackend {
    pub fn is_supported() -> bool {
        true //TODO detect support
    }
}

fn convert_result(ret: i32) -> std::io::Result<i32> {
    if ret >= 0 {
        Ok(ret)
    } else {
        Err(std::io::Error::from_raw_os_error(-ret))
    }
}

struct IouOp {
    state: IouOpState,
}

impl IouOp {
    fn new(op: Entry) -> Self {
        IouOp {
            state: IouOpState::Inactive(op),
        }
    }
}

thread_local! {
    static CURRENT_TASK_ID: Cell<Option<TaskId>> = Cell::new(None);
    static CURRENT_RESULT: Cell<Option<i32>> = Cell::new(None);
}

enum IouOpState {
    Inactive(Entry),
    Submitted,
    Completed,
}

lazy_static! {
    static ref IO_URING: io_uring::concurrent::IoUring = {
        let num_entries = 1 << SIZE_POW;
        let uring = io_uring::IoUring::new(num_entries).unwrap();
        uring.concurrent()
    };
}

impl Future for IouOp {
    type Output = std::io::Result<i32>;
    fn poll(self: Pin<&mut Self>, _: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut tmp = IouOpState::Completed;
        std::mem::swap(&mut this.state, &mut tmp);
        match tmp {
            IouOpState::Inactive(op) => {
                let op = op.user_data(CURRENT_TASK_ID.with(|i| i.get()).unwrap().0);
                let sub = IO_URING.submission();
                let res = unsafe { sub.push(op) };
                assert!(res.is_ok());
                IO_URING.submit().unwrap();
                this.state = IouOpState::Submitted;
                Poll::Pending
            }
            IouOpState::Submitted => {
                this.state = IouOpState::Completed;
                let res = CURRENT_RESULT
                    .with(|r| r.get())
                    .expect("Should only be polled when result is ready");
                Poll::Ready(convert_result(res))
            }
            IouOpState::Completed => {
                panic!("Polling completed IouOp");
            }
        }
    }
}

async fn open(path: &Path) -> std::io::Result<File> {
    let path = path.as_os_str();
    let path = CString::new(OsStrExt::as_bytes(path)).unwrap();

    let op = Openat::new(0.into(), path.as_ref().as_ptr()).build();

    IouOp::new(op)
        .await
        .map(|fd| unsafe { File::from_raw_fd(fd) })
}

async fn close(file: File) -> std::io::Result<()> {
    let fd = file.into_raw_fd();
    let op = Close::new(Target::Fd(fd)).build();

    IouOp::new(op).await.map(|_| ())
}

async fn read(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let fd = file.as_raw_fd();
    let op = Read::new(Target::Fd(fd), buf.as_mut_ptr(), buf.len() as _).build();

    IouOp::new(op).await.map(|i| i as usize)
}

async fn foo(path: &Path) -> std::io::Result<()> {
    let mut file = open(path).await?;

    let mut buf = vec![0u8; 256];
    let _num_read = read(&mut file, &mut buf).await?;

    println!("{}", String::from_utf8_lossy(&buf));

    close(file).await?;

    Ok(())
}

async fn bar(path: &Path) {
    if let Err(e) = foo(path).await {
        eprintln!("Error: {}", e);
    }
}

fn dummy_raw_waker() -> RawWaker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        dummy_raw_waker()
    }

    let vtable = &RawWakerVTable::new(clone, no_op, no_op, no_op);
    RawWaker::new(0 as *const (), vtable)
}

fn dummy_waker() -> Waker {
    let raw = dummy_raw_waker();
    unsafe { Waker::from_raw(raw) }
}

#[derive(Copy, Clone, Hash, PartialEq, Debug, Eq)]
struct TaskId(u64);

impl TaskId {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        TaskId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

struct Task<'future> {
    id: TaskId,
    future: Pin<Box<dyn Future<Output = ()> + 'future>>,
}

impl<'future> Task<'future> {
    fn new(f: impl Future<Output = ()> + 'future) -> Task<'future> {
        Task {
            id: TaskId::new(),
            future: Box::pin(f),
        }
    }

    fn id(&self) -> TaskId {
        self.id
    }
}

struct Executor<'tasks> {
    tasks: HashMap<TaskId, Task<'tasks>>,
    waker: Waker,
}

impl<'tasks> Executor<'tasks> {
    fn new() -> Self {
        Executor {
            tasks: HashMap::new(),
            waker: dummy_waker(),
        }
    }

    fn spawn(&mut self, f: impl Future<Output = ()> + 'tasks) {
        let mut task = Task::new(f);

        let task_id = task.id();

        CURRENT_TASK_ID.with(|r| r.set(Some(task_id)));

        let mut context = Context::from_waker(&self.waker);
        match task.future.as_mut().poll(&mut context) {
            Poll::Pending => {}
            Poll::Ready(_) => return,
        }

        let prev = self.tasks.insert(task_id, task);
        assert!(prev.is_none(), "Id somehow reused");
    }

    fn next_result(&mut self) -> (TaskId, i32) {
        let result = loop {
            if let Some(res) = IO_URING.completion().pop() {
                break res;
            }
            println!("Wait...");
        };
        let id = result.user_data();
        let task_result = result.result();
        (TaskId(id), task_result)
    }

    fn poll(&mut self) {
        let (task_id, result) = self.next_result();
        let task = self.tasks.get_mut(&task_id).expect("Invalid task id");

        CURRENT_RESULT.with(|r| r.set(Some(result)));
        CURRENT_TASK_ID.with(|r| r.set(Some(task_id)));

        eprintln!("Running id {}", task_id.0);

        let mut context = Context::from_waker(&self.waker);
        match task.future.as_mut().poll(&mut context) {
            Poll::Pending => {}
            Poll::Ready(_) => {
                self.tasks.remove(&task_id).expect("Invalid task_id");
            }
        }
    }

    fn run(&mut self) {
        while !self.tasks.is_empty() {
            self.poll();
        }
    }
}

impl Backend for IoUringBackend {
    fn run(_dir: PathBuf, _matcher: impl Matcher) {
        let mut executor = Executor::new();

        executor.spawn(bar(Path::new("/home/dominik/foo.c")));
        executor.spawn(bar(Path::new("/home/dominik/foo.txt")));

        executor.run();
    }
}
