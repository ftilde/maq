use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;

mod generic;
mod io_uring;

use generic::GenericBackend;
use io_uring::IoUringBackend;

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
    #[structopt(help = "base directory for recursive mail search", parse(from_os_str))]
    dir: PathBuf,
}

#[derive(Default)]
struct AddrData {
    name_variants: HashMap<String, u64>,
    occurences: u64,
}

fn to_io_err<I>(_: I) -> std::io::Error {
    std::io::Error::from(std::io::ErrorKind::InvalidInput)
}

trait Matcher: Clone + Send + 'static {
    fn new(pattern: String) -> Self;
    fn matches(&self, s: &str) -> bool;
}

#[derive(Clone)]
struct CaseInsensitiveMatcher<M>(M);
impl<M: Matcher> Matcher for CaseInsensitiveMatcher<M> {
    fn new(pattern: String) -> Self {
        CaseInsensitiveMatcher(M::new(pattern.to_lowercase()))
    }
    fn matches(&self, s: &str) -> bool {
        self.0.matches(&s.to_lowercase())
    }
}

#[derive(Clone)]
struct SubstringMatcher(String);
impl Matcher for SubstringMatcher {
    fn new(pattern: String) -> Self {
        SubstringMatcher(pattern)
    }
    fn matches(&self, s: &str) -> bool {
        s.contains(&self.0)
    }
}

#[derive(Clone)]
struct FuzzyMatcher(String);
impl Matcher for FuzzyMatcher {
    fn new(pattern: String) -> Self {
        FuzzyMatcher(pattern)
    }
    fn matches(&self, s: &str) -> bool {
        fuzzy_matcher::skim::fuzzy_match(s, &self.0).is_some()
    }
}

trait Backend {
    fn run(dir: PathBuf, matcher: impl Matcher);
}

fn run_backend<B: Backend>() {
    let options = Options::from_args();

    // Somewhat ugly, but what we need for static dispatch
    if options.fuzzy {
        if options.ignore_case {
            B::run(
                options.dir,
                CaseInsensitiveMatcher::<FuzzyMatcher>::new(options.search_string),
            )
        } else {
            B::run(options.dir, FuzzyMatcher::new(options.search_string))
        }
    } else {
        if options.ignore_case {
            B::run(
                options.dir,
                CaseInsensitiveMatcher::<SubstringMatcher>::new(options.search_string),
            )
        } else {
            B::run(options.dir, SubstringMatcher::new(options.search_string))
        }
    }
}

fn main() {
    if IoUringBackend::is_supported() {
        run_backend::<IoUringBackend>();
    } else {
        run_backend::<GenericBackend>();
    }
}
