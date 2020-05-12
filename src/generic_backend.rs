use crate::common::{process_mail_header, AddrCollection, HeaderParseResult, Mails};
use crate::{Backend, Matcher};
use std::io::Read;
use std::path::PathBuf;

fn process_mail(
    p: PathBuf,
    matcher: &impl Matcher,
    addrs: &mut AddrCollection,
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
        match process_mail_header(&buf[..total_read], &mut pos, matcher, addrs) {
            HeaderParseResult::Done => break,
            HeaderParseResult::NeedMore => {}
        }
    }
    Ok(())
}

fn process_mails(matcher: impl Matcher, mails: &Mails) -> AddrCollection {
    let mut addrs = AddrCollection::new();
    while let Some(path) = mails.get() {
        let _ = process_mail(path, &matcher, &mut addrs);
    }
    addrs
}

pub struct GenericBackend;

impl Backend for GenericBackend {
    fn run(dir: PathBuf, matcher: impl Matcher) {
        let mails = &*Box::leak(Box::new(Mails::new(dir)));
        let num_threads = num_cpus::get();

        let threads = (1..num_threads)
            .into_iter()
            .map(|_| {
                let m = matcher.clone();
                std::thread::spawn(move || process_mails(m, mails))
            })
            .collect::<Vec<_>>();

        let mut addrs = process_mails(matcher, mails);
        for thread in threads {
            addrs.merge(thread.join().unwrap());
        }
        addrs.print();
    }
}
