use crate::common::{find_mails, parse_header_line, AddrCollection};
use crate::{Backend, Matcher};
use bstr::io::BufReadExt;
use crossbeam_channel::{bounded, Receiver, Sender};
use mailparse::SingleInfo;
use std::io::BufReader;
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
type ProcessOutput = SingleInfo;

fn process_mail(
    p: PathBuf,
    matcher: &impl Matcher,
    sender: &Sender<ProcessOutput>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(p)?;
    let expected_header_size = 4 * 1024; // 4KB
    let reader = BufReader::with_capacity(expected_header_size, file);

    reader.for_byte_line(|line| {
        if line.is_empty() {
            // Empty line: End of mail header
            return Ok(false);
        }
        /* TODO: use sub slice patterns when stable in 1.42
        let is_addr_line = match line.as_slice() {
            [b'F', b'r', b'o', b'm', b':', b' ', ..] => true,
            _ => false,
        };*/
        let is_addr_line = line.len() > 6 && &line[0..6] == b"From: "
            || line.len() > 4 && &line[0..4] == b"To: "
            || line.len() > 4 && &line[0..4] == b"CC: "
            || line.len() > 5 && &line[0..5] == b"BCC: ";

        if is_addr_line {
            let (addrs, _) = parse_header_line(&line, matcher.clone())?;
            for addr in addrs {
                sender
                    .send(addr.clone())
                    .expect("Receiver outlives senders");
            }
        }
        Ok(true)
    })?;
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
