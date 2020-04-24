use crate::common::{
    find_mails, process_mail_header, AddrCollection, HeaderParseResult, ProcessOutput,
};
use crate::{Backend, Matcher};
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
    sender: Sender<ProcessOutput>,
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
        match process_mail_header(&buf, &mut pos, matcher, &sender) {
            HeaderParseResult::Done => break,
            HeaderParseResult::NeedMore => {}
        }
    }

    close(file).await?;

    Ok(())
}

async fn process(path: PathBuf, matcher: &impl Matcher, sender: Sender<ProcessOutput>) {
    if let Err(e) = process_mail(&path, matcher, sender).await {
        eprintln!("Error: {}", e);
    }
}

fn process_results(receiver: Receiver<ProcessOutput>) {
    let mut addrs = AddrCollection::new();
    while let Ok(addr) = receiver.recv() {
        addrs.add(addr);
    }

    addrs.print();
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

fn process_mails(
    matcher: impl Matcher,
    receiver: Receiver<ProcessInput>,
    sender: Sender<ProcessOutput>,
) {
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
            executor.spawn(process(m, &matcher, sender.clone()));
        }
    }

    while executor.has_tasks() {
        if executor.poll().is_ready() {
            if let Some(m) = get_mail() {
                executor.spawn(process(m, &matcher, sender.clone()));
            }
        }
    }
}

impl Backend for IoUringBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let num_threads = num_cpus::get();
        //let num_threads = 1;
        let (path_sender, path_receiver) = bounded(num_threads);
        let (addrinfo_sender, addrinfo_receiver) = bounded(num_threads);

        let _ = std::thread::spawn(|| {
            find_and_batch_mails(dir, path_sender);
        });

        for _ in 0..num_threads {
            let m = matcher.clone();
            let s = addrinfo_sender.clone();
            let r = path_receiver.clone();
            let _ = std::thread::spawn(move || process_mails(m, r, s));
        }

        std::mem::drop(path_receiver);
        std::mem::drop(addrinfo_sender);

        process_results(addrinfo_receiver);
    }
}
