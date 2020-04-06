use crate::{Backend, Matcher};
use std::path::PathBuf;

pub struct IoUringBackend;

impl IoUringBackend {
    pub fn is_supported() -> bool {
        false
    }
}

impl Backend for IoUringBackend {
    fn run(_dir: PathBuf, _matcher: impl Matcher) {
        todo!();
    }
}
