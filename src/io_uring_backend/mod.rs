use crate::common::{find_mails, process_mail_header, AddrCollection, HeaderParseResult};
use crate::{Backend, Matcher};
use core::cell::RefCell;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::path::{Path, PathBuf};

mod executor;

use executor::{close, open, read_to_vec, Executor};

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

type ProcessInput = Vec<PathBuf>;

fn find_and_batch_mails(dir: PathBuf, sender: Sender<ProcessInput>) {
    let batch_size = 64;
    let mut batch = Vec::with_capacity(batch_size);
    for mail in find_mails(dir) {
        batch.push(mail);
        if batch.len() == batch_size {
            sender.send(batch).expect("Receivers outlive sender");

            batch = Vec::with_capacity(batch_size);
        }
    }
    sender.send(batch).expect("Receivers outlive sender");
}

fn process_mails(matcher: impl Matcher, receiver: Receiver<ProcessInput>) -> AddrCollection {
    let addrs = RefCell::new(AddrCollection::new());
    let mut batch = Vec::new();
    let mut get_mail = || {
        if batch.is_empty() {
            if let Ok(addrs) = receiver.recv() {
                batch = addrs;
            }
        }

        batch.pop()
    };
    let num_parallel_mails = 1 << 6;
    let mut executor = Executor::new(num_parallel_mails);
    for _ in 0..num_parallel_mails {
        if let Some(m) = get_mail() {
            executor.spawn(process(m, &matcher, &addrs));
        }
    }

    while executor.has_tasks() {
        if executor.poll().is_ready() {
            if let Some(m) = get_mail() {
                executor.spawn(process(m, &matcher, &addrs));
            }
        }
    }
    std::mem::drop(executor);
    addrs.into_inner()
}

impl Backend for IoUringBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let num_threads = num_cpus::get();
        //let num_threads = 1;
        let (path_sender, path_receiver) = bounded(num_threads);

        let _ = std::thread::spawn(|| {
            find_and_batch_mails(dir, path_sender);
        });

        let threads = (1..num_threads)
            .into_iter()
            .map(|_| {
                let m = matcher.clone();
                let r = path_receiver.clone();
                std::thread::spawn(move || process_mails(m, r))
            })
            .collect::<Vec<_>>();

        let mut addrs = process_mails(matcher, path_receiver);
        for thread in threads {
            addrs.merge(thread.join().unwrap());
        }
        addrs.print();
    }
}
