use crate::{to_io_err, AddrData, Backend, Matcher};
use bstr::io::BufReadExt;
use crossbeam_channel::{bounded, Receiver, Sender};
use mailparse::{addrparse, parse_header, MailAddr, SingleInfo};
use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use walkdir::WalkDir;

fn find_mails(dir: PathBuf, sender: Sender<ProcessInput>) {
    let batch_size = 64;
    let mut batch = Vec::with_capacity(batch_size);
    for entry in WalkDir::new(dir) {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                eprintln!("Dir error: {}", e);
                continue;
            }
        };

        if entry.file_type().is_dir() {
            continue;
        }

        batch.push(entry.into_path());
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
            let header = parse_header(&line).map_err(to_io_err)?;
            for addr in &*addrparse(&header.0.get_value().map_err(to_io_err)?).map_err(to_io_err)? {
                let addr = match addr {
                    MailAddr::Single(a) => a,
                    MailAddr::Group(_) => return Ok(false),
                };
                if matcher.matches(&addr.addr)
                    || addr
                        .display_name
                        .as_ref()
                        .map(|n| matcher.matches(n))
                        .unwrap_or(false)
                {
                    sender
                        .send(addr.clone())
                        .expect("Receiver outlives senders");
                }
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
    let mut addrs = HashMap::new();
    while let Ok(addr) = receiver.recv() {
        let data = addrs
            .entry(addr.addr.to_owned())
            .or_insert(AddrData::default());
        data.occurences += 1;
        if let Some(name) = &addr.display_name {
            *data.name_variants.entry(name.to_owned()).or_insert(0) += 1;
        }
    }

    let mut addrs = addrs.into_iter().collect::<Vec<_>>();
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

pub struct GenericBackend;

impl Backend for GenericBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let num_threads = num_cpus::get();
        let (path_sender, path_receiver) = bounded(num_threads);
        let (addrinfo_sender, addrinfo_receiver) = bounded(num_threads);

        let _ = std::thread::spawn(|| {
            find_mails(dir, path_sender);
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