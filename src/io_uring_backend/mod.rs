use core::cell::RefCell;
use crate::{Backend, Matcher};
use std::path::{Path, PathBuf};
use crate::common::{find_mails, parse_header_line, AddrCollection};

mod executor;

use executor::{Executor, open, close, read_to_vec};

pub struct IoUringBackend;

impl IoUringBackend {
    pub fn is_supported() -> bool {
        false
    }
}

#[derive(Copy, Clone)]
enum ParseState {
    Begin,
    From(usize),
    To(usize),
    CC(usize),
    BCC(usize),
    FoundAddr { begin: usize },
    Uninteresting,
    End,
}

fn advance_state(current_byte: u8, current_pos: usize, state: ParseState) -> ParseState {
    match (state, current_byte) {
        (ParseState::Begin, b'F') => ParseState::From(0),
        (ParseState::From(0), b'r') => ParseState::From(1),
        (ParseState::From(1), b'o') => ParseState::From(2),
        (ParseState::From(2), b'm') => ParseState::From(3),
        (ParseState::From(3), b':') => ParseState::FoundAddr { begin: current_pos - 4 },
        (ParseState::Begin, b'T') => ParseState::To(0),
        (ParseState::To(0), b'o') => ParseState::To(1),
        (ParseState::To(1), b':') => ParseState::FoundAddr { begin: current_pos - 2 },
        (ParseState::Begin, b'C') => ParseState::CC(0),
        (ParseState::CC(0), b'C') => ParseState::CC(1),
        (ParseState::CC(1), b':') => ParseState::FoundAddr { begin: current_pos - 2 },
        (ParseState::Begin, b'B') => ParseState::BCC(0),
        (ParseState::BCC(0), b'C') => ParseState::BCC(1),
        (ParseState::BCC(1), b'C') => ParseState::BCC(2),
        (ParseState::BCC(2), b':') => ParseState::FoundAddr { begin: current_pos - 3 },
        (ParseState::Begin, b'\n') => ParseState::End,
        (_, b'\n') => ParseState::Begin,
        (_, _) => ParseState::Uninteresting,
    }
}

async fn process_mail(path: &Path, matcher: &impl Matcher, addr_collection: &RefCell<AddrCollection>) -> std::io::Result<()> {
    let mut file = open(path).await?;

    let read_block_size = 4*1024; //4KB
    let mut buf = Vec::new();

    let mut state = ParseState::Begin;

    let mut pos = 0;
    'outer: loop {
        let num_read = read_to_vec(&mut file, &mut buf, read_block_size).await?;
        //eprintln!("File pos: {}", file.offset());
        if num_read == 0 {
            break;
        }
        'inner: while pos < buf.len() {
            let byte = buf[pos];
            state = advance_state(byte, pos, state);
            match state {
                ParseState::End => break 'outer,
                ParseState::FoundAddr{ begin } => {
                    if let Ok((addrs, next_pos)) = parse_header_line(&buf[begin..], matcher.clone()) {
                        state = ParseState::Begin;
                        if next_pos == buf.len() {
                            // Might not be the whole addr line, better read more and try again
                            pos = begin;
                            continue 'outer;
                        } else {
                            pos = begin + next_pos;
                            let mut addr_collection = addr_collection.borrow_mut();
                            for addr in addrs {
                                addr_collection.add(addr);
                            }
                            continue 'inner;
                        }
                    }
                },
                _ => {}
            }
            pos += 1;
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
                if let Some(m) = mails.next() { //TODO use take or something like that
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
