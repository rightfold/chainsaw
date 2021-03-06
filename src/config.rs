use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/*----------------------------------------------------------------------------*/

#[derive(Debug)]
pub enum Error {
    ParseError,
    MissingStore,
    IOError(io::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IOError(error)
    }
}

/*----------------------------------------------------------------------------*/

#[derive(Debug, Eq, PartialEq)]
pub struct Config {
    pub store: PathBuf,
    pub logs: HashSet<String>,
}

impl Config {
    pub fn new_from_file<P>(path: P) -> Result<Self, Error>
        where P: AsRef<Path>{
        let mut buf_reader = BufReader::new(try!(File::open(path)));
        Self::new_from_buf_read(&mut buf_reader)
    }

    pub fn new_from_buf_read<B>(buf_read: &mut B) -> Result<Self, Error>
        where B: BufRead {
        let mut store = None;
        let mut logs = HashSet::new();

        for line_result in buf_read.lines() {
            let full_line = try!(line_result);
            match full_line.trim() {
                "" => {},
                line if line.starts_with("#") => {},
                line if line.starts_with("LOG ") =>
                    { logs.insert(line[4..].trim_left().to_string()); },
                line if line.starts_with("STORE ") =>
                    store = Some(PathBuf::from(&line[6..].trim_left())),
                _ => return Err(Error::ParseError),
            }
        }

        Ok(Config{store: try!(store.ok_or(Error::MissingStore)), logs: logs})
    }
}

/*----------------------------------------------------------------------------*/

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::*;

    fn test_logs(source: &[u8], expected: &[&str]) {
        let mut cursor = Cursor::new(source);
        let config = Config::new_from_buf_read(&mut cursor).unwrap();
        assert_eq!(config.logs, expected.iter().map(|s| s.to_string()).collect());
    }

    #[test]
    fn test_empty() {
        test_logs(b"", &[]);
    }

    #[test]
    fn test_single() {
        test_logs(b"LOG foo\n", &["foo"]);
    }

    #[test]
    fn test_many() {
        test_logs(b"LOG foo\nLOG bar\n", &["foo", "bar"]);
    }

    #[test]
    fn test_comment() {
        test_logs(b"LOG foo\n#LOG bar\n", &["foo"]);
    }
}
