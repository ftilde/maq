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
    search_string: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(p)?;
    let reader = BufReader::new(file);

    for line in reader.byte_lines() {
        let line = line?;
        if line.len() > 6 && &line[0..6] == b"From: " {
            //TODO CC etc.

            let header = parse_header(&line)?;
            for addr in &*addrparse(&header.0.get_value()?)? {
                let addr = match addr {
                    MailAddr::Single(a) => a,
                    MailAddr::Group(_) => return Ok(()),
                };
                if addr.addr.contains(search_string)
                    || addr
                        .display_name
                        .as_ref()
                        .map(|n| n.contains(search_string))
                        .unwrap_or(false)
                {
                    let data = addrs
                        .entry(addr.addr.to_owned())
                        .or_insert(AddrData::default());
                    data.occurences += 1;
                    if let Some(name) = &addr.display_name {
                        if name.contains(search_string) {
                            *data.name_variants.entry(name.to_owned()).or_insert(0) += 1;
                        }
                    }
                }
            }
            return Ok(());
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
