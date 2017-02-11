use libc::{c_char, c_int, c_void, free, malloc, size_t};
use std::io;
use std::mem::transmute;
use std::slice;

/*----------------------------------------------------------------------------*/

#[link(name = "zmq")]
extern {
    fn zmq_ctx_new() -> *mut c_void;
    fn zmq_ctx_term(context: *mut c_void) -> c_int;
    fn zmq_socket(context: *mut c_void, type_: c_int) -> *mut c_void;
    fn zmq_close(socket: *mut c_void) -> c_int;
    fn zmq_setsockopt(socket: *mut c_void, option_name: c_int, option_value: *const c_void, option_len: size_t) -> c_int;
    fn zmq_bind(socket: *mut c_void, endpoint: *const c_char) -> c_int;
    fn zmq_connect(socket: *mut c_void, endpoint: *const c_char) -> c_int;
    fn zmq_msg_init(msg: *mut [u8; 64]) -> c_int;
    fn zmq_msg_init_size(msg: *mut [u8; 64], size: size_t) -> c_int;
    fn zmq_msg_close(msg: *mut [u8; 64]) -> c_int;
    fn zmq_msg_data(msg: *mut [u8; 64]) -> *mut c_void;
    fn zmq_msg_size(msg: *mut [u8; 64]) -> size_t;
    fn zmq_msg_more(msg: *mut [u8; 64]) -> c_int;
    fn zmq_recvmsg(socket: *mut c_void, msg: *mut [u8; 64], flags: c_int) -> c_int;
    fn zmq_sendmsg(socket: *mut c_void, msg: *mut [u8; 64], flags: c_int) -> c_int;
}

/*----------------------------------------------------------------------------*/

pub struct Context(*mut c_void);

impl Context {
    pub fn new() -> io::Result<Self> {
        let handle = unsafe { zmq_ctx_new() };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Context(handle))
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { zmq_ctx_term(self.0) };
    }
}

unsafe impl Send for Context { }
unsafe impl Sync for Context { }

/*----------------------------------------------------------------------------*/

pub struct Socket(*mut c_void);

pub enum SocketType {
    PUB  = 1,
    SUB  = 2,
    PUSH = 8,
    PULL = 7,
}

impl Socket {
    pub fn new(context: &mut Context, type_: SocketType) -> io::Result<Self> {
        let handle = unsafe { zmq_socket(context.0, type_ as c_int) };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Socket(handle))
        }
    }

    pub fn subscribe(&mut self, prefix: &[u8]) -> io::Result<()> {
        let status = unsafe { zmq_setsockopt(self.0, 6, transmute(prefix.as_ptr()), prefix.len()) };
        if status == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn bind(&mut self, endpoint: &mut Vec<u8>) -> io::Result<()> {
        endpoint.push(0);
        let status = unsafe { zmq_bind(self.0, transmute(endpoint.as_ptr())) };
        if status == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn connect(&mut self, mut endpoint: &mut Vec<u8>) -> io::Result<()> {
        endpoint.push(0);
        let status = unsafe { zmq_connect(self.0, transmute(endpoint.as_ptr())) };
        if status == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn receive(&mut self, message: &mut Message) -> io::Result<()> {
        let status = unsafe { zmq_recvmsg(self.0, message.0, 0) };
        if status == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn send(&mut self, message: &mut Message, send_more: bool) -> io::Result<()> {
        let flags = if send_more { 2 } else { 0 };
        let status = unsafe { zmq_sendmsg(self.0, message.0, flags) };
        if status == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { zmq_close(self.0) };
    }
}

unsafe impl Send for Socket { }

/*----------------------------------------------------------------------------*/

pub struct Message(*mut [u8; 64]);

impl Message {
    pub fn new() -> Self {
        let handle = unsafe { transmute(malloc(64)) };
        unsafe { zmq_msg_init(handle) };
        Message(handle)
    }

    pub fn from_data(data: &[u8]) -> Self {
        let handle = unsafe { transmute(malloc(64)) };
        unsafe { zmq_msg_init_size(handle, data.len()) };
        let mut message = Message(handle);
        message.data().copy_from_slice(data);
        message
    }

    pub fn data<'a>(&'a mut self) -> &'a mut [u8] {
        let data = unsafe { zmq_msg_data(transmute(self.0)) };
        let len = unsafe { zmq_msg_size(transmute(self.0)) };
        unsafe { slice::from_raw_parts_mut(transmute(data), len) }
    }

    pub fn more(&self) -> bool {
        (unsafe { zmq_msg_more(transmute(self)) }) == 1
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        unsafe { zmq_msg_close(self.0) };
        unsafe { free(transmute(self.0)) };
    }
}

/*----------------------------------------------------------------------------*/

#[cfg(test)]
mod tests {
    use std::thread::spawn;
    use super::*;

    #[test]
    fn test_zmq() {
        let mut context = Context::new().unwrap();

        let mut client = Socket::new(&mut context, SocketType::PUSH).unwrap();
        client.connect(&mut b"inproc://foo".to_vec()).unwrap();

        let mut server = Socket::new(&mut context, SocketType::PULL).unwrap();
        server.bind(&mut b"inproc://foo".to_vec()).unwrap();

        spawn(move || {
            client.send(&mut Message::from_data(b"hello"), false).unwrap();
        });

        let mut response = Message::new();
        server.receive(&mut response).unwrap();

        assert_eq!(response.data(), b"hello");
    }
}
