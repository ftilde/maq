use crate::Matcher;
use core::sync::atomic::{AtomicUsize, Ordering};
use mailparse::{addrparse, parse_header, MailAddr, SingleInfo};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use walkdir::WalkDir;

pub fn find_mails(dir: PathBuf) -> impl Iterator<Item = PathBuf> {
    WalkDir::new(dir).into_iter().filter_map(|entry| {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                eprintln!("Dir error: {}", e);
                return None;
            }
        };

        if entry.file_type().is_dir() {
            return None;
        }

        Some(entry.into_path())
    })
}

pub struct Mails {
    mails: Vec<PathBuf>,
    current: AtomicUsize,
}

impl Mails {
    pub fn new(dir: PathBuf) -> Self {
        Mails {
            mails: find_mails(dir).collect(),
            current: AtomicUsize::new(0),
        }
    }
    pub fn get(&self) -> Option<PathBuf> {
        // We could do some unsafe magic here to avoid the clone, but so far this is very much not
        // a bottle neck.
        let index = self.current.fetch_add(1, Ordering::SeqCst);
        if index < self.mails.len() {
            Some(self.mails[index].clone())
        } else {
            None
        }
    }
}

fn to_io_err<I>(_: I) -> std::io::Error {
    std::io::Error::from(std::io::ErrorKind::InvalidInput)
}

pub enum HeaderParseResult {
    NeedMore,
    Done,
}

pub fn process_mail_header(
    buf: &[u8],
    pos: &mut usize,
    matcher: &impl Matcher,
    addr_collection: &mut AddrCollection,
) -> HeaderParseResult {
    const MIN_OFFSET: usize = 5;
    while *pos + MIN_OFFSET < buf.len() {
        let interesting = match buf[*pos..] {
            [b'F', b'r', b'o', b'm', b':', ..] => true,
            [b'T', b'o', b':', ..] => true,
            [b'C', b'C', b':', ..] => true,
            [b'B', b'C', b'C', b':', ..] => true,
            [b'\n', ..] => return HeaderParseResult::Done,
            _ => false,
        };

        if interesting {
            if let Ok((addrs, next_pos)) = parse_header_line(&buf[*pos..], matcher.clone()) {
                if next_pos == buf.len() {
                    // Might not be the whole addr line, better read more and try again
                    return HeaderParseResult::NeedMore;
                } else {
                    *pos += next_pos;
                    for addr in addrs {
                        addr_collection.add(addr);
                    }
                    continue;
                }
            }
        }
        if let Some(next_offset) = memchr::memchr(b'\n', &buf[*pos..]) {
            *pos += next_offset + 1;
        } else {
            return HeaderParseResult::NeedMore;
        }
    }
    HeaderParseResult::NeedMore
}

pub fn parse_header_line<'matcher>(
    line: &[u8],
    matcher: impl Matcher,
) -> Result<(impl Iterator<Item = SingleInfo> + 'matcher, usize), std::io::Error> {
    let header = parse_header(&line).map_err(to_io_err)?;
    let iter = addrparse(&header.0.get_value())
        .map_err(to_io_err)?
        .into_inner();
    Ok((
        iter.into_iter().filter_map(move |addr| {
            let addr = match addr {
                MailAddr::Single(a) => a,
                MailAddr::Group(_) => return None,
            };
            if matcher.matches(&addr.addr)
                || addr
                    .display_name
                    .as_ref()
                    .map(|n| matcher.matches(n))
                    .unwrap_or(false)
            {
                Some(addr.clone())
            } else {
                None
            }
        }),
        header.1,
    ))
}

#[derive(Default)]
struct AddrData {
    name_variants: HashMap<String, u64>,
    occurences: u64,
}

pub struct AddrCollection {
    addrs: HashMap<String, AddrData>,
}

impl AddrCollection {
    pub fn add(&mut self, addr: SingleInfo) {
        let data = self
            .addrs
            .entry(addr.addr.to_owned())
            .or_insert(AddrData::default());
        data.occurences += 1;
        if let Some(name) = &addr.display_name {
            *data.name_variants.entry(name.to_owned()).or_insert(0) += 1;
        }
    }

    pub fn merge(&mut self, other: AddrCollection) {
        for (addr, other_data) in other.addrs {
            if let Some(this_data) = self.addrs.get_mut(&addr) {
                this_data.occurences += other_data.occurences;
                for (name, occurences) in other_data.name_variants {
                    *this_data.name_variants.entry(name).or_insert(0) += occurences;
                }
            } else {
                self.addrs.insert(addr, other_data);
            }
        }
    }

    pub fn new() -> Self {
        AddrCollection {
            addrs: HashMap::new(),
        }
    }

    pub fn print(self) {
        let mut addrs = self.addrs.into_iter().collect::<Vec<_>>();
        // Sort (reverse) so that high number of occurences are on top
        addrs.sort_by_key(|(_, data)| u64::max_value() - data.occurences);

        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();

        let _ = writeln!(stdout, "");
        for (addr, data) in addrs {
            let name_variant = data
                .name_variants
                .iter()
                .max_by_key(|(_, n)| *n)
                .map(|(name, _)| name.as_str())
                .unwrap_or("");

            let _ = writeln!(stdout, "{}\t{}", addr, name_variant);
        }
    }
}
