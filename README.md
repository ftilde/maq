# maq (maildir-address-query)

maq can be used to extract mail addresses from mails stored in maildir format.

Compared to [mail-query](https://github.com/pbrisbin/mail-query) it has the following additional features:

* Header fields `CC`, `BCC` and `TO` are included in the search results in addition to `FROM`
* Results are sorted from most to least frequent appearances
* The most frequent variation is used as the display name
* Search ergonomics can be improved using the `--fuzzy` and `--ignore-case` flags
* 7-bit ASCII encoded MIME-headers are decoded automatically
* Mail processing is parallelized and thus (possibly, depending on your hardware) faster
* An io_uring backend, which may be beneficial if you have few cores and/or limited ram for disk cache

In contrast to mail-query, maq does not (yet?) support regex search.

## Usage

```
$ maq --help
maq 0.1.0
ftilde <ftilde@protonmail.com>
maildir-address-query: Collect and query mail addresses from maildirs

USAGE:
    maq [FLAGS] [OPTIONS] <dir>

FLAGS:
    -f, --fuzzy          Apply fuzzy matching (instead of absolute)
    -h, --help           Prints help information
    -i, --ignore-case    Ignore case
    -V, --version        Prints version information

OPTIONS:
    -s, --search <search-string>    Search string [default: ]

ARGS:
    <dir>    base directory for recursive mail search
```

Add the following to your `muttrc` for case-insensitive, fuzzy address completion in mutt:

```muttrc
set query_command = "/path/to/maq -i -f -s %s /path/to/maildir"
```

## Building

maq is written in Rust and needs a working installation of cargo to build.

```
$ git clone https://github.com/ftilde/maq
$ cd maq
$ cargo build --release
$ target/release/maq
```

## Licensing

`maq` is released under the MIT license.
