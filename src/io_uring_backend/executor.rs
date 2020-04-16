use core::sync::atomic::AtomicUsize;
use core::cell::Cell;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::{RawWaker, RawWakerVTable, Waker};
use io_uring::opcode::{types::Target, Close, Openat, Read};
use io_uring::squeue::Entry;
use lazy_static::lazy_static;
use std::ffi::CString;
use std::future::Future;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::path::Path;
use std::sync::atomic::Ordering;

const QUEUE_SIZE_POW: usize = 8;
const QUEUE_SIZE: usize = 1 << QUEUE_SIZE_POW;

struct AtomicFixedStorage<T> {
    storage: Box<[AtomicUsize]>,
    next_free: AtomicUsize,
    num_elms: AtomicUsize,
    _marker: std::marker::PhantomData<T>,
}

const NO_MORE_STORAGE: usize = std::usize::MAX;

impl<T> AtomicFixedStorage<T> {
    fn new(size: usize) -> Self {
        let mut storage = Vec::new();
        for i in 1..size {
            storage.push(AtomicUsize::new(i));
        }
        storage.push(AtomicUsize::new(NO_MORE_STORAGE));
        AtomicFixedStorage {
            storage: storage.into_boxed_slice(),
            next_free: AtomicUsize::new(0),
            num_elms: AtomicUsize::new(0),
            _marker: std::marker::PhantomData,
        }
    }

    fn num_elms(&self) -> usize {
        self.num_elms.load(Ordering::SeqCst)
    }

    fn allocate(&self) -> Option<StorageID> {
        self.num_elms.fetch_add(1, Ordering::SeqCst);
        loop {
            let old = self.next_free.load(Ordering::SeqCst);
            if old == NO_MORE_STORAGE {
                self.num_elms.fetch_sub(1, Ordering::SeqCst);
                return None;
            }
            let new = self.storage[old].load(Ordering::SeqCst);
            if self.next_free.compare_and_swap(old, new, Ordering::SeqCst) == old {
                self.storage[old].store(0, Ordering::SeqCst);
                return Some(StorageID(old));
            }
        }
    }

    unsafe fn free(&self, id: StorageID) {
        let _ = self.get_usize(id).map(|p| Box::from_raw(p as *mut T)); // drop if present

        loop {
            let old = self.next_free.load(Ordering::SeqCst);
            self.storage[id.0].store(old, Ordering::SeqCst);

            let new = id.0;

            if self.next_free.compare_and_swap(old, new, Ordering::SeqCst) == old {
                break;
            }
        }

        self.num_elms.fetch_sub(1, Ordering::SeqCst);
    }

    unsafe fn set(&self, id: StorageID, val: Box<T>) {
        let _ = self.replace(id, val);
    }

    unsafe fn replace(&self, id: StorageID, val: Box<T>) -> Option<Box<T>> {
        let old = self.get_usize(id).map(|p| Box::from_raw(p as _));
        self.storage[id.0].store(Box::into_raw(val) as usize, Ordering::SeqCst);
        old
    }

    fn get_usize(&self, id: StorageID) -> Option<usize> {
        match self.storage[id.0].load(Ordering::SeqCst) {
            0 => None,
            p => Some(p)
        }
    }

    //unsafe fn get(&self, id: StorageID) -> Option<&T> {
    //    self.get_usize(id).map(|p| &*(p as *const T))
    //}

    unsafe fn get_mut(&self, id: StorageID) -> Option<&mut T> {
        self.get_usize(id).map(|p| &mut *(p as *mut T))
    }
}

impl<T> Drop for AtomicFixedStorage<T> {
    fn drop(&mut self) {
        //TODO for now we just leak, meh...
    }
}

#[derive(Copy, Clone, Debug)]
pub struct StorageID(usize);

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
    static CURRENT_TASK_ID: Cell<Option<StorageID>> = Cell::new(None);
    static CURRENT_RESULT: Cell<Option<i32>> = Cell::new(None);
}

enum IouOpState {
    Inactive(Entry),
    Submitted,
    Completed,
}

lazy_static! {
    static ref IO_URING: io_uring::concurrent::IoUring = {
        let uring = io_uring::IoUring::new(QUEUE_SIZE as _).unwrap();
        uring.concurrent()
    };

    pub static ref EXECUTOR: Executor<'static> = {
        Executor::new()
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
                let op = op.user_data(CURRENT_TASK_ID.with(|i| i.get()).unwrap().0 as _);
                let sub = IO_URING.submission();
                //match unsafe { sub.push(op) } {
                //    Ok(_) => this.state = IouOpState::Submitted,
                //    Err(op) => this.state = IouOpState::Inactive(op),
                //}
                //Poll::Pending
                let res = unsafe { sub.push(op) };
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

    let op = Openat::new(Target::Fd(libc::AT_FDCWD), path.as_ref().as_ptr()).build();

    IouOp::new(op)
        .await
        .map(|fd| unsafe { std::fs::File::from_raw_fd(fd).into() })
}

pub async fn close(file: File) -> std::io::Result<()> {
    let fd = file.inner.into_raw_fd();
    let op = Close::new(Target::Fd(fd)).build();

    IouOp::new(op).await.map(|_| ())
}

#[allow(unused)]
pub async fn read(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let fd = file.inner.as_raw_fd();
    let op = Read::new(Target::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
        .offset(file.offset as _)
        .build();

    match IouOp::new(op).await {
        Ok(num_written) => {
            let num_written = num_written as usize;
            file.offset += num_written;
            Ok(num_written)
        }
        Err(e) => Err(e),
    }
}

pub async fn read_to_vec(
    file: &mut File,
    buf: &mut Vec<u8>,
    max_to_read: usize,
) -> std::io::Result<usize> {
    let append_pos = buf.len();
    let op = {
        let fd = file.inner.as_raw_fd();
        let additional_storage = append_pos.saturating_sub(buf.capacity()) + max_to_read;
        buf.reserve(additional_storage);
        let write_pos = unsafe { buf.as_mut_ptr().add(append_pos) };
            Read::new(Target::Fd(fd), write_pos, max_to_read as _)
            .offset(file.offset as _)
            .build()
    };

    match IouOp::new(op).await {
        Ok(num_written) => {
            let num_written = num_written as usize;
            unsafe { buf.set_len(append_pos + num_written) };
            file.offset += num_written;
            Ok(num_written)
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
    unsafe { Waker::from_raw(raw) }
}

struct Task<'future> {
    future: Pin<Box<dyn Future<Output = ()> + 'future + Send + Sync>>,
}

impl<'future> Task<'future> {
    fn new(f: impl Future<Output = ()> + 'future + Send + Sync) -> Task<'future> {
        Task {
            future: Box::pin(f),
        }
    }
}

pub struct Executor<'tasks> {
    tasks: AtomicFixedStorage<Task<'tasks>>,
    waker: Waker,
}

impl<'tasks> Executor<'tasks> {
    fn new() -> Self {
        Executor {
            tasks: AtomicFixedStorage::new(QUEUE_SIZE/2),
            waker: dummy_waker(),
        }
    }

    pub fn spawn(&self, f: impl Future<Output = ()> + 'tasks + Send + Sync) -> Result<Poll<StorageID>,()>{
        unsafe {
            let id = self.tasks.allocate().ok_or(())?;
            Ok(self.spawn_at(id, f))
        }
    }
    pub unsafe fn spawn_at(&self, id: StorageID, f: impl Future<Output = ()> + 'tasks + Send + Sync) -> Poll<StorageID> {
        let task = Task::new(f);
        self.tasks.set(id, Box::new(task));

        CURRENT_TASK_ID.with(|r| r.set(Some(id)));

        let mut context = Context::from_waker(&self.waker);
        let task = self.tasks.get_mut(id).unwrap();
        match task.future.as_mut().poll(&mut context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(id),
        }
    }

    fn next_result(&self) -> Option<(StorageID, i32)> {
        let result = loop {
            if let Some(res) = IO_URING.completion().pop() {
                break res;
            }
            IO_URING.submit().unwrap(); //TODO figure out where to submit best
            if let Some(res) = IO_URING.completion().pop() {
                break res;
            } else {
                return None;
            }
        };
        let id = result.user_data();
        //eprintln!("ID: {}", id);
        let task_result = result.result();
        Some((StorageID(id as _), task_result))
    }

    pub fn poll(&self) -> Option<Poll<StorageID>> {
        let (id, result) = self.next_result()?;
        //let mut task = loop {
        //    if let Some(t) = self.tasks.get_mut(&task_id) {
        //        break t;
        //    }
        //};
        let task = unsafe { self.tasks.get_mut(id).unwrap() };

        CURRENT_RESULT.with(|r| r.set(Some(result)));
        CURRENT_TASK_ID.with(|r| r.set(Some(id)));

        //eprintln!("Running id {}", task_id.0);

        let mut context = Context::from_waker(&self.waker);
        match task.future.as_mut().poll(&mut context) {
            Poll::Pending => Some(Poll::Pending),
            Poll::Ready(_) => Some(Poll::Ready(id)),
        }
    }

    pub unsafe fn dispose(&self, id: StorageID) {
        self.tasks.free(id)
    }

    //pub fn run(&mut self) {
    //    while self.has_tasks() {
    //        let _ = self.poll();
    //    }
    //}

    pub fn has_tasks(&self) -> bool {
        self.tasks.num_elms() > 0
    }
}
