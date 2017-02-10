use std::fs::{create_dir, File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_HEADER: [u8; 16] = [
    0x90, 0x00, 0xd1, 0x09, // magic number
    0, 0, 0, 0, // major version
    0, 0, 0, 0, // minor version
    0, 0, 0, 1, // patch version
];

const RECORD_COUNT_LIMIT: usize = 8192;

pub struct Appender<'s, 'l, C> {
    clock: C,
    store: &'s Path,
    log:   &'l str,
    file:  File,

    record_count: usize,
}

pub fn create(store: &Path, log: &str) -> io::Result<()> {
    create_dir(store.join(log))
}

pub fn open_for_append<'s, 'l, C>(clock: C, store: &'s Path, log: &'l str) -> io::Result<Appender<'s, 'l, C>>
    where C: Fn() -> SystemTime {
    let file = try!(Appender::open(&clock, store, log));
    Ok(Appender{clock: clock, store: store, log: log, file: file, record_count: 0})
}

impl<'s, 'l, C> Appender<'s, 'l, C> where C: Fn() -> SystemTime {
    fn open(clock: &C, store: &'s Path, log: &'l str) -> io::Result<File> {
        let time = clock().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let path = store.join(log).join(time.to_string());
        let mut file = try!(OpenOptions::new().create_new(true).append(true).open(path));
        try!(file.write_all(&FILE_HEADER));
        Ok(file)
    }

    pub fn append(&mut self, record: &[u8]) -> io::Result<()> {
        try!(self.file.write_all(&binary::encode_u32(record.len() as u32)));
        try!(self.file.write_all(record));
        try!(self.file.write_all(&binary::encode_u32(0)));

        self.rotate_if_necessary()
    }

    fn rotate_if_necessary(&mut self) -> io::Result<()> {
        self.record_count += 1;
        if self.record_count > RECORD_COUNT_LIMIT {
            self.file = try!(Self::open(&self.clock, self.store, self.log));
            self.record_count = 0;
        }
        Ok(())
    }
}

mod binary {
    pub fn encode_u32(i: u32) -> [u8; 4] {
        [
            (i >>  0) as u8,
            (i >>  8) as u8,
            (i >> 16) as u8,
            (i >> 24) as u8,
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::{metadata, remove_dir_all};
    use std::path::PathBuf;
    use std::time::UNIX_EPOCH;
    use super::*;

    fn test_store() -> PathBuf {
        temp_dir()
    }

    fn test_log_dir(log: &str) -> PathBuf {
        test_store().join(log)
    }

    #[test]
    fn test_create() {
        let log = "chainsaw_test_create";
        remove_dir_all(&test_log_dir(log)).unwrap_or(());
        assert!(create(&test_store(), log).is_ok());
        assert!(metadata(&test_log_dir(log))
                .map(|m| m.file_type().is_dir())
                .unwrap_or(false));
    }

    #[test]
    fn test_open_for_append() {
        let log = "chainsaw_test_open_for_append";
        remove_dir_all(&test_log_dir(log)).unwrap_or(());
        create(&test_store(), log).unwrap();
        assert!(open_for_append(|| UNIX_EPOCH, &test_store(), log).is_ok());
        assert!(test_log_dir(log).join("0").is_file());
    }
}
