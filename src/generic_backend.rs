use crate::common::{
    find_mails, process_mail_header, AddrCollection, HeaderParseResult, ProcessOutput,
};
use crate::{Backend, Matcher};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::io::Read;
use std::path::PathBuf;

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

type ProcessInput = Vec<PathBuf>;

fn process_mail(
    p: PathBuf,
    matcher: &impl Matcher,
    sender: &Sender<ProcessOutput>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(p)?;
    let expected_header_size = 4 * 1024; // 4KB

    let mut buf = Vec::new();
    let mut total_read = 0;
    let mut pos = 0;
    loop {
        buf.resize(total_read + expected_header_size, 0);
        let num_read = file.read(&mut buf[total_read..])?;
        if num_read == 0 {
            break;
        }
        total_read += num_read;
        match process_mail_header(&buf[..total_read], &mut pos, matcher, &sender) {
            HeaderParseResult::Done => break,
            HeaderParseResult::NeedMore => {}
        }
    }
    Ok(())
}

fn process_mails(
    matcher: impl Matcher,
    receiver: Receiver<ProcessInput>,
    sender: Sender<ProcessOutput>,
) {
    while let Ok(paths) = receiver.recv() {
        for path in paths {
            let _ = process_mail(path, &matcher, &sender);
        }
    }
}

fn process_results(receiver: Receiver<ProcessOutput>) {
    let mut addrs = AddrCollection::new();
    while let Ok(addr) = receiver.recv() {
        addrs.add(addr);
    }

    addrs.print();
}

pub struct GenericBackend;

impl Backend for GenericBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let num_threads = num_cpus::get();
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
