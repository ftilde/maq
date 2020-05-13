use crate::common::{process_mail_header, AddrCollection, HeaderParseResult, Mails};
use crate::{Backend, Matcher};
use core::cell::RefCell;
use std::path::{Path, PathBuf};

mod executor;

use executor::{close, open, read_to_vec, Executor, ExecutorPollResult};

pub struct IoUringBackend;

impl IoUringBackend {
    pub fn is_supported() -> bool {
        false
    }
}

async fn process_mail(
    path: &Path,
    matcher: &impl Matcher,
    addr_collection: &RefCell<AddrCollection>,
) -> std::io::Result<()> {
    let mut file = open(path).await?;

    let read_block_size = 4 * 1024; //4KB
    let mut buf = Vec::new();

    let mut pos = 0;
    loop {
        let num_read = read_to_vec(&mut file, &mut buf, read_block_size).await?;
        //eprintln!("File pos: {}", file.offset());
        if num_read == 0 {
            break;
        }
        let mut addr_collection = addr_collection.borrow_mut();
        match process_mail_header(&buf, &mut pos, matcher, &mut *addr_collection) {
            HeaderParseResult::Done => break,
            HeaderParseResult::NeedMore => {}
        }
    }

    close(file).await?;

    Ok(())
}

async fn process(path: PathBuf, matcher: &impl Matcher, addrs: &RefCell<AddrCollection>) {
    if let Err(e) = process_mail(&path, matcher, addrs).await {
        eprintln!("Error: {}", e);
    }
}

fn process_mails(matcher: impl Matcher, mails: &Mails) -> AddrCollection {
    let addrs = RefCell::new(AddrCollection::new());
    let num_parallel_mails = 1usize << 6;
    let mut executor = Executor::new(num_parallel_mails as u32);

    if let Some(m) = mails.get() {
        executor.spawn(process(m, &matcher, &addrs));
    }

    while executor.has_tasks() {
        match executor.poll(false) {
            ExecutorPollResult::Finished => {
                if let Some(m) = mails.get() {
                    executor.spawn(process(m, &matcher, &addrs));
                }
            }
            ExecutorPollResult::WouldBlock => {
                if executor.num_tasks() < num_parallel_mails {
                    if let Some(m) = mails.get() {
                        executor.spawn(process(m, &matcher, &addrs));
                    }
                }
                if let ExecutorPollResult::Finished = executor.poll(true) {
                    if let Some(m) = mails.get() {
                        executor.spawn(process(m, &matcher, &addrs));
                    }
                }
            }
            ExecutorPollResult::Polled => {}
        }
    }
    std::mem::drop(executor);
    addrs.into_inner()
}

impl Backend for IoUringBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let mails = &*Box::leak(Box::new(Mails::new(dir)));
        let num_threads = num_cpus::get();
        //let num_threads = 1;

        let threads = (1..num_threads)
            .into_iter()
            .map(|_| {
                let m = matcher.clone();
                std::thread::spawn(move || process_mails(m, mails))
            })
            .collect::<Vec<_>>();

        let mut addrs = process_mails(matcher, mails);
        for thread in threads {
            addrs.merge(thread.join().unwrap());
        }
        addrs.print();
    }
}
