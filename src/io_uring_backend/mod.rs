use core::task::Poll;
use crate::io_uring_backend::executor::EXECUTOR;
use mailparse::SingleInfo;
use crossbeam_channel::{bounded, Receiver, Sender};
use crate::common::{find_mails, parse_header_line, AddrCollection};
use crate::{Backend, Matcher};
use std::path::{Path, PathBuf};

mod executor;

use executor::{close, open, read_to_vec};

pub struct IoUringBackend;

impl IoUringBackend {
    pub fn is_supported() -> bool {
        false
    }
}

async fn process_mail(
    path: &Path,
    matcher: impl Matcher,
    sender: Sender<ProcessOutput>,
) -> std::io::Result<()> {
    let mut file = open(path).await?;

    let read_block_size = 4 * 1024; //4KB
    let mut buf = Vec::new();

    let mut n = 0;

    let mut pos = 0;
    'outer: loop {
        let num_read = read_to_vec(&mut file, &mut buf, read_block_size).await?;
        //eprintln!("File pos: {}", file.offset());
        if num_read == 0 {
            break;
        }
        const MIN_OFFSET: usize = 5;
        'inner: while pos + MIN_OFFSET < buf.len() {
            let interesting = match buf[pos..] {
                [b'F', b'r', b'o', b'm', b':', ..] => true,
                [b'T', b'o', b':', ..] => true,
                [b'C', b'C', b':', ..] => true,
                [b'B', b'C', b'C', b':', ..] => true,
                [b'\n', ..] => break 'outer,
                _ => false,
            };

            n += 1;
            if n == 1000 {
                println!("{}", path.to_string_lossy());
            }
            if interesting {
                if let Ok((addrs, next_pos)) = parse_header_line(&buf[pos..], matcher.clone()) {
                    if next_pos == buf.len() {
                        // Might not be the whole addr line, better read more and try again
                        continue 'outer;
                    } else {
                        pos += next_pos;
                        for addr in addrs {
                            let _ = sender.send(addr);
                        }
                        continue 'inner;
                    }
                }
            }
            if let Some(next_offset) = memchr::memchr(b'\n', &buf[pos..]) {
                pos += next_offset + 1;
            } else {
                continue 'outer;
            }
        }
    }

    close(file).await?;

    Ok(())
}

async fn process(path: PathBuf, matcher: impl Matcher, sender: Sender<ProcessOutput>) {
    if let Err(e) = process_mail(&path, matcher, sender).await {
        eprintln!("Error: {}", e);
    }
}

type ProcessInput = Vec<PathBuf>;
type ProcessOutput = SingleInfo;

fn process_mails(
    matcher: impl Matcher,
    receiver: Receiver<ProcessInput>,
    sender: Sender<ProcessOutput>,
) {
    let mut batch = Vec::new();
    let mut returned = None;
    let mut get_mail = |returned: &mut Option<PathBuf>| {
        if let Some(r) = returned.take() {
            return Some(r);
        }
        if batch.is_empty() {
            if let Ok(addrs) = receiver.recv() {
                batch = addrs;
            }
        }

        batch.pop()
    };
    {
        while let Some(m) = get_mail(&mut returned) {
            match EXECUTOR.spawn(process(m.clone(), matcher.clone(), sender.clone())) {
                Ok(Poll::Pending) => {}
                Ok(Poll::Ready(_)) => panic!("Immediately ready should not happen"),
                Err(_) => {
                    returned = Some(m);
                    break;
                }
            }
        }

        loop {
            if let Some(res) = EXECUTOR.poll() {
                if let Poll::Ready(id) = res {
                    if let Some(m) = get_mail(&mut returned) {
                        if unsafe { EXECUTOR.spawn_at(id, process(m, matcher.clone(), sender.clone())) }.is_ready() {
                            panic!("Immediately ready should not happen");
                        }
                    } else {
                        unsafe { EXECUTOR.dispose(id) };
                    }
                }
            } else {
                if let Some(m) = get_mail(&mut returned) {
                    match EXECUTOR.spawn(process(m.clone(), matcher.clone(), sender.clone())) {
                        Ok(Poll::Pending) => {}
                        Ok(Poll::Ready(_)) => panic!("Immediately ready should not happen"),
                        Err(_) => returned = Some(m),
                    }
                } else {
                    if !EXECUTOR.has_tasks() {
                        break;
                    }
                }
            }
        }
    }
}

fn find_and_batch_mails(dir: PathBuf, sender: Sender<ProcessInput>) {
    let batch_size = 16;
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

fn process_results(receiver: Receiver<ProcessOutput>) {
    let mut addrs = AddrCollection::new();
    while let Ok(addr) = receiver.recv() {
        addrs.add(addr);
    }

    addrs.print();
}

impl Backend for IoUringBackend {
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
