use bstr::io::BufReadExt;
use mailparse::{addrparse, parse_header, MailAddr};
use std::collections::HashMap;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use structopt::StructOpt;
use walkdir::WalkDir;

#[derive(StructOpt)]
#[structopt(author, about)]
struct Options {
    #[structopt(
        short = "s",
        long = "search",
        help = "Search string",
        default_value = ""
    )]
    search_string: String,
    #[structopt(short = "i", long = "ignore-case", help = "Ignore case")]
    ignore_case: bool,
    #[structopt(
        short = "f",
        long = "fuzzy",
        help = "Apply fuzzy matching (instead of absolute)"
    )]
    fuzzy: bool,
    #[structopt(help = "Directory to sweep through", parse(from_os_str))]
    dir: PathBuf,
}

#[derive(Default)]
struct AddrData {
    name_variants: HashMap<String, u64>,
    occurences: u64,
}

fn process(
    addrs: &mut HashMap<String, AddrData>,
    p: &Path,
    matcher: &impl Matcher,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(p)?;
    let reader = BufReader::new(file);

    for line in reader.byte_lines() {
        let line = line?;
        if line.is_empty() {
            // End of header (i think?)
            return Ok(());
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
            let header = parse_header(&line)?;
            for addr in &*addrparse(&header.0.get_value()?)? {
                let addr = match addr {
                    MailAddr::Single(a) => a,
                    MailAddr::Group(_) => return Ok(()),
                };
                if matcher.matches(&addr.addr)
                    || addr
                        .display_name
                        .as_ref()
                        .map(|n| matcher.matches(n))
                        .unwrap_or(false)
                {
                    let data = addrs
                        .entry(addr.addr.to_owned())
                        .or_insert(AddrData::default());
                    data.occurences += 1;
                    if let Some(name) = &addr.display_name {
                        if matcher.matches(name) {
                            *data.name_variants.entry(name.to_owned()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

trait Matcher {
    fn new(pattern: String) -> Self;
    fn matches(&self, s: &str) -> bool;
}

struct CaseInsensitiveMatcher<M>(M);
impl<M: Matcher> Matcher for CaseInsensitiveMatcher<M> {
    fn new(pattern: String) -> Self {
        CaseInsensitiveMatcher(M::new(pattern.to_lowercase()))
    }
    fn matches(&self, s: &str) -> bool {
        self.0.matches(&s.to_lowercase())
    }
}

struct SubstringMatcher(String);
impl Matcher for SubstringMatcher {
    fn new(pattern: String) -> Self {
        SubstringMatcher(pattern)
    }
    fn matches(&self, s: &str) -> bool {
        s.contains(&self.0)
    }
}
struct FuzzyMatcher(String);
impl Matcher for FuzzyMatcher {
    fn new(pattern: String) -> Self {
        FuzzyMatcher(pattern)
    }
    fn matches(&self, s: &str) -> bool {
        fuzzy_matcher::skim::fuzzy_match(s, &self.0).is_some()
    }
}

fn run(dir: PathBuf, matcher: impl Matcher) {
    let mut addrs = HashMap::new();

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

        match process(&mut addrs, entry.path(), &matcher) {
            Ok(_) => {}
            Err(_e) => {
                //eprintln!("Process error: {}", e);
                continue;
            }
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

fn main() {
    let options = Options::from_args();

    // Somewhat ugly, but what we need for compile time dispatch
    if options.fuzzy {
        if options.ignore_case {
            run(
                options.dir,
                CaseInsensitiveMatcher::<FuzzyMatcher>::new(options.search_string),
            )
        } else {
            run(options.dir, FuzzyMatcher::new(options.search_string))
        }
    } else {
        if options.ignore_case {
            run(
                options.dir,
                CaseInsensitiveMatcher::<SubstringMatcher>::new(options.search_string),
            )
        } else {
            run(options.dir, SubstringMatcher::new(options.search_string))
        }
    }
}
