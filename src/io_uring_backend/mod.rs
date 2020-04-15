use crate::common::{find_mails, parse_header_line, AddrCollection};
use crate::{Backend, Matcher};
use core::cell::RefCell;
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
                        let mut addr_collection = addr_collection.borrow_mut();
                        for addr in addrs {
                            addr_collection.add(addr);
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

async fn process(path: PathBuf, matcher: &impl Matcher, addrs: &RefCell<AddrCollection>) {
    if let Err(e) = process_mail(&path, matcher, addrs).await {
        eprintln!("Error: {}", e);
    }
}

impl Backend for IoUringBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let addrs = RefCell::new(AddrCollection::new());
        {
            let mut executor = Executor::new();
            let mut mails = find_mails(dir);
            let num_parallel_mails = 64;
            for _ in 0..num_parallel_mails {
                if let Some(m) = mails.next() {
                    //TODO use take or something like that
                    executor.spawn(process(m, &matcher, &addrs));
                }
            }

            while executor.has_tasks() {
                if executor.poll().is_ready() {
                    if let Some(m) = mails.next() {
                        executor.spawn(process(m, &matcher, &addrs));
                    }
                }
            }
        }

        addrs.into_inner().print();
    }
}
