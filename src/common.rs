use crate::Matcher;
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

fn to_io_err<I>(_: I) -> std::io::Error {
    std::io::Error::from(std::io::ErrorKind::InvalidInput)
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
