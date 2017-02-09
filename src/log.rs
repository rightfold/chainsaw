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

pub struct Appender(File);

pub fn create(store: &Path, log: &str) -> io::Result<()> {
    create_dir(store.join(log))
}

pub fn open_for_append<Clock>(clock: Clock, store: &Path, log: &str) -> io::Result<Appender>
    where Clock: FnOnce() -> SystemTime {
    let time = clock().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let path = store.join(log).join(time.to_string());
    let mut file = try!(OpenOptions::new().create_new(true).append(true).open(path));
    try!(file.write_all(&FILE_HEADER));
    Ok(Appender(file))
}
