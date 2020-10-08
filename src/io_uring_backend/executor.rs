use core::cell::Cell;
use core::pin::Pin;
use core::sync::atomic::AtomicU64;
use core::task::Context;
use core::task::Poll;
use core::task::{RawWaker, RawWakerVTable, Waker};
use io_uring::opcode::{types::Target, Close, Openat, Read};
use io_uring::squeue::Entry;
use std::collections::HashMap;
use std::ffi::CString;
use std::future::Future;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::path::Path;
use std::sync::atomic::Ordering;

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
    /// `op` must be a valid entry (see io_uring docs)
    unsafe fn new(op: Entry) -> Self {
        IouOp {
            state: IouOpState::Inactive(op),
        }
    }
}

thread_local! {
    static CURRENT_TASK_ID: Cell<Option<TaskId>> = Cell::new(None);
    static CURRENT_RESULT: Cell<Option<i32>> = Cell::new(None);
    static IO_URING: Cell<*mut io_uring::IoUring> = Cell::new(std::ptr::null_mut());
}

enum IouOpState {
    Inactive(Entry),
    Submitted,
    Completed,
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
                let uring_ptr = IO_URING.with(|t| t.get());
                assert!(!uring_ptr.is_null(), "No uring");
                // Safety: IO_URING is a thread local (=> no send/sync required) and is only ever
                // set (i.e. != null) in `poll` and `spawn` (which both require a mutable safe
                // reference.
                let uring = unsafe { &mut *uring_ptr };
                let res = {
                    let mut sub = uring.submission().available();
                    // Safety: op is safe via new invariant.
                    unsafe { sub.push(op) }
                };

                //if uring.submission().len() > 0 {
                //    let _foo = uring.submit().unwrap();
                //    //println!("BAR: {}", _foo);
                //}
                assert!(res.is_ok(), "Queue is full!");
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

pub struct File {
    inner: std::fs::File,
    offset: usize,
}

impl From<std::fs::File> for File {
    fn from(inner: std::fs::File) -> Self {
        File { inner, offset: 0 }
    }
}

impl File {
    #[allow(unused)]
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl std::ops::Deref for File {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for File {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub async fn open(path: &Path) -> std::io::Result<File> {
    let path = path.as_os_str();
    let path = CString::new(OsStrExt::as_bytes(path)).unwrap();

    let op = Openat::new(libc::AT_FDCWD, path.as_ref().as_ptr()).build();

    // Safety: There are no safety concerns for the Openat operation.
    unsafe { IouOp::new(op) }
        .await
        // Safety: Openat returns a valid fd or an error, but the error case is already handled
        // in the future implementation of IouOp.
        .map(|fd| unsafe { std::fs::File::from_raw_fd(fd).into() })
}

pub async fn close(file: File) -> std::io::Result<()> {
    let fd = file.inner.into_raw_fd();
    let op = Close::new(fd).build();

    // Safety: file is a valid file, so fd is valid as well.
    unsafe { IouOp::new(op) }.await.map(|_| ())
}

pub async fn read_to_vec(
    file: &mut File,
    buf: Vec<u8>,
    max_to_read: usize,
) -> std::io::Result<(usize, Vec<u8>)> {
    let mut buf = std::mem::ManuallyDrop::new(buf);
    let fd = file.inner.as_raw_fd();
    let append_pos = buf.len();
    let additional_storage = append_pos.saturating_sub(buf.capacity()) + max_to_read;
    buf.reserve(additional_storage);
    // Safety: We just reserved that space.
    let write_pos = unsafe { buf.as_mut_ptr().add(append_pos) };
    let op = Read::new(Target::Fd(fd), write_pos, max_to_read as _)
        .offset(file.offset as _)
        .build();

    // Safety:
    // 1. fd corresponds to a valid std::fs::File
    // 2. buf[write_pos..write_pos+max_to_read] is actually part of the buffer (see calculation of
    //    `additional_storage` and the following `reserve` call.
    // 3. If this future is dropped while the IouOp is in flight, buf is not invalidated and merely
    //    leaked (Not ideal, but still safe. This should never happen in this application anyway.)
    match unsafe { IouOp::new(op) }.await {
        Ok(num_written) => {
            let num_written = num_written as usize;
            // Safety: We have reserved the space (see above) and we have read the specified number
            // of additional bytes.
            unsafe { buf.set_len(append_pos + num_written) };
            file.offset += num_written;
            Ok((num_written, std::mem::ManuallyDrop::into_inner(buf)))
        }
        Err(e) => Err(e),
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
    // Safety: The dummy waker literally does nothing and thus upholds all cantracts of
    // `RawWaker`/`RawWakerVTable`.
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

pub struct Executor<'tasks> {
    tasks: HashMap<TaskId, Task<'tasks>>,
    uring: io_uring::IoUring,
    waker: Waker,
    queue_size: usize,
}

impl<'tasks> Executor<'tasks> {
    pub fn fully_supported(self) -> std::io::Result<Self> {
        async fn test_run() -> std::io::Result<()> {
            let path = Path::new("/dev/null");
            let mut file = open(path).await?;
            let _ = read_to_vec(&mut file, Vec::new(), 1).await?;
            close(file).await?;
            Ok(())
        }
        let mut res = Ok(());
        let mut executor = self;
        let executor = {
            executor.spawn(async {
                if let Err(e) = test_run().await {
                    res = Err(e);
                }
            });
            executor.run_to_completion()
        };
        res.map(|_| executor)
    }

    pub fn run_to_completion<'a>(mut self) -> Executor<'a> {
        while self.has_tasks() {
            self.poll(true);
        }
        Executor {
            tasks: HashMap::new(),
            uring: self.uring,
            waker: self.waker,
            queue_size: self.queue_size,
        }
    }

    pub fn new(queue_size: u32) -> Self {
        Executor {
            tasks: HashMap::new(),
            uring: io_uring::IoUring::new(queue_size).unwrap(),
            waker: dummy_waker(),
            queue_size: queue_size as _,
        }
    }

    pub fn max_tasks(&self) -> usize {
        self.queue_size
    }

    pub fn num_tasks(&self) -> usize {
        self.tasks.len()
    }

    pub fn spawn(&mut self, f: impl Future<Output = ()> + 'tasks) {
        let mut task = Task::new(f);

        let task_id = task.id();

        CURRENT_TASK_ID.with(|r| r.set(Some(task_id)));
        let uring = &mut self.uring;
        IO_URING.with(|t| t.set(uring as _));

        let mut context = Context::from_waker(&self.waker);
        match task.future.as_mut().poll(&mut context) {
            Poll::Pending => {}
            Poll::Ready(_) => return,
        }

        IO_URING.with(|t| t.set(std::ptr::null_mut()));

        let prev = self.tasks.insert(task_id, task);
        assert!(prev.is_none(), "Id somehow reused");
    }

    fn next_result(&mut self, wait: bool) -> Option<(TaskId, i32)> {
        let result = loop {
            if !wait {
                let _foo = self.uring.submit().unwrap();
                //println!("Not wait: {}", _foo);
            }
            {
                if let Some(res) = self.uring.completion().available().into_iter().next() {
                    break res;
                }
            }
            if !wait {
                //println!("Not ready");
                return None;
            }
            let _foo = self.uring.submit_and_wait(1).unwrap(); //TODO figure out where to submit best
                                                               //println!("Wait: {}", _foo);
            break self
                .uring
                .completion()
                .available()
                .into_iter()
                .next()
                .unwrap();
        };
        let id = result.user_data();
        let task_result = result.result();
        Some((TaskId(id), task_result))
    }

    pub fn poll(&mut self, wait: bool) -> ExecutorPollResult {
        let (task_id, result) = if let Some(r) = self.next_result(wait) {
            r
        } else {
            return ExecutorPollResult::WouldBlock;
        };
        let uring = &mut self.uring;
        let task = self.tasks.get_mut(&task_id).expect("Invalid task id");

        CURRENT_RESULT.with(|r| r.set(Some(result)));
        CURRENT_TASK_ID.with(|r| r.set(Some(task_id)));
        IO_URING.with(|t| t.set(uring as _));

        //eprintln!("Running id {}", task_id.0);

        let mut context = Context::from_waker(&self.waker);
        let res = match task.future.as_mut().poll(&mut context) {
            Poll::Pending => ExecutorPollResult::Polled,
            Poll::Ready(_) => {
                self.tasks.remove(&task_id).expect("Invalid task_id");
                ExecutorPollResult::Finished
            }
        };

        IO_URING.with(|t| t.set(std::ptr::null_mut()));
        res
    }

    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }
}

pub enum ExecutorPollResult {
    WouldBlock,
    Polled,
    Finished,
}
