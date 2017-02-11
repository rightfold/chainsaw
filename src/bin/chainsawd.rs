extern crate chainsaw;

use chainsaw::config;
use chainsaw::config::Config;
use chainsaw::log;
use chainsaw::zmq;
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{JoinHandle, spawn};
use std::time::SystemTime;

/*----------------------------------------------------------------------------*/

const INPROC_ADDRESS: &'static [u8] = b"inproc://pub";

/*----------------------------------------------------------------------------*/

#[derive(Debug)]
enum Error {
    MissingConfigPath,
    ConfigError(config::Error),
    IOError(io::Error),
}

impl From<config::Error> for Error {
    fn from(error: config::Error) -> Self {
        Error::ConfigError(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IOError(error)
    }
}

/*----------------------------------------------------------------------------*/

fn main() {
    safe_main().unwrap();
}

fn safe_main() -> Result<(), Error> {
    let config_path = try!(env::args().nth(1).ok_or(Error::MissingConfigPath));
    let config = try!(Config::new_from_file(config_path));

    let store = Arc::new(config.store);

    let zmq = Arc::new(try!(zmq::Context::new()));
    let mut pub_ = try!(make_pub(&zmq));

    for logger in start_loggers(&store, &zmq, config.logs.iter().cloned()) {
        logger.join().unwrap().unwrap();
    }

    Ok(())
}

fn make_pub(zmq: &zmq::Context) -> io::Result<zmq::Socket> {
    let mut pub_ = try!(zmq::Socket::new(zmq, zmq::SocketType::PUB));
    try!(pub_.bind(&mut INPROC_ADDRESS.to_vec()));
    Ok(pub_)
}

fn start_loggers<I>(store: &Arc<PathBuf>, zmq: &Arc<zmq::Context>, logs: I)
    -> Vec<JoinHandle<io::Result<()>>>
    where I: Iterator<Item=String> {
    logs
    .map(|log| {
        let store = store.clone();
        let zmq = zmq.clone();
        spawn(move || { run_logger(&store, &zmq, &log) })
    })
    .collect()
}

fn run_logger(store: &Path, zmq: &zmq::Context, log: &str) -> io::Result<()> {
    let mut sub = try!(zmq::Socket::new(zmq, zmq::SocketType::SUB));
    try!(sub.connect(&mut INPROC_ADDRESS.to_vec()));
    try!(sub.subscribe(log.as_bytes()));

    let clock = || SystemTime::now();
    let mut appender = try!(log::open_for_append(clock, store, log));

    let mut message = zmq::Message::new();
    loop {
        try!(sub.receive(&mut message));
        try!(appender.append(message.data()));
    }
}
