use mailparse::{addrparse, parse_mail, MailAddr};
use std::collections::HashMap;
use std::io::Read;
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
    #[structopt(help = "Directory to sweep through", parse(from_os_str))]
    dir: PathBuf,
}

fn process(
    addrs: &mut HashMap<String, u64>,
    p: &Path,
    search_string: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(p)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let mail = parse_mail(&buf)?;

    for header in mail.headers {
        let key = header.get_key()?;
        if key == "To" || key == "From" || key == "CC" || key == "BCC" {
            let value = header.get_value()?;
            for addr in &*addrparse(&value)? {
                let addr = match addr {
                    MailAddr::Single(a) => a,
                    MailAddr::Group(_) => return Ok(()),
                };
                if addr.addr.contains(search_string) {
                    *addrs.entry(addr.addr.to_owned()).or_insert(0) += 1;
                }
            }
        }
    }
    Ok(())
}

fn main() {
    let options = Options::from_args();

    let mut addrs = HashMap::new();

    for entry in WalkDir::new(options.dir) {
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

        match process(&mut addrs, entry.path(), &options.search_string) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Process error: {}", e);
                continue;
            }
        }
    }

    let mut addrs = addrs.into_iter().collect::<Vec<_>>();
    addrs.sort_by_key(|(_, count)| *count);
    for (addr, _) in addrs {
        println!("{}", addr);
    }
}
