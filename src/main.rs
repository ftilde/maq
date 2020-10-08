use std::path::PathBuf;
use structopt::StructOpt;

mod common;
mod generic_backend;
mod io_uring_backend;

use generic_backend::GenericBackend;
use io_uring_backend::IoUringBackend;

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
    #[structopt(long = "generic-backend", help = "Force generic backend")]
    generic_backend: bool,
    #[structopt(help = "base directory for recursive mail search", parse(from_os_str))]
    dir: PathBuf,
}

pub trait Matcher: Clone + Send + 'static {
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
        fuzzy_matcher::skim::SkimMatcherV2::default()
            .fuzzy(s, &self.0, false)
            .is_some()
    }
}

#[derive(Debug)]
enum BackendError {
    NotSupported,
}
trait Backend: Sized {
    fn construct() -> Result<Self, BackendError>;
    fn run(self, dir: PathBuf, matcher: impl Matcher);
}

fn run_backend(backend: impl Backend, options: Options) {
    // Somewhat ugly, but what we need for static dispatch
    if options.fuzzy {
        if options.ignore_case {
            backend.run(
                options.dir,
                CaseInsensitiveMatcher::<FuzzyMatcher>::new(options.search_string),
            )
        } else {
            backend.run(options.dir, FuzzyMatcher::new(options.search_string))
        }
    } else {
        if options.ignore_case {
            backend.run(
                options.dir,
                CaseInsensitiveMatcher::<SubstringMatcher>::new(options.search_string),
            )
        } else {
            backend.run(options.dir, SubstringMatcher::new(options.search_string))
        }
    }
}

fn main() {
    let options = Options::from_args();
    if options.generic_backend {
        run_backend(GenericBackend::construct().unwrap(), options);
    } else {
        if let Ok(backend) = IoUringBackend::construct() {
            run_backend(backend, options);
        } else {
            eprintln!("IO-uring backend is not (fully) on your system supported. (Linux Kernel version 5.6 or above is required.) Falling back to generic backend.");
            run_backend(GenericBackend::construct().unwrap(), options);
        }
    }
}
