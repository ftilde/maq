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

pub trait Matcher: Clone + Send + 'static + Sync {
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

fn run_backend<B: Backend>(options: Options) {
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
    let options = Options::from_args();
    if !options.generic_backend && IoUringBackend::is_supported() {
        run_backend::<IoUringBackend>(options);
    } else {
        run_backend::<GenericBackend>(options);
    }
}
