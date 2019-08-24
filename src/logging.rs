// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::mem;
use std::path::Path;

// In the C++ source file which the cpp macro will generate make sure the relevant includes are
// present
cpp! {{
#include <rocksdb/env.h>

using namespace rocksdb;

class RustLogger : public rocksdb::Logger {
public:
    explicit RustLogger(const InfoLogLevel log_level, void* rust_boxed_logger)
        : rocksdb::Logger(log_level),
        rust_boxed_logger_(rust_boxed_logger)
    {}

    virtual ~RustLogger() {
        //Free the boxed logger
        rust!(RustLogger_free_boxed_logger [rust_boxed_logger_ : *mut CppLoggerWrapper as "void*"] {
            unsafe {
                Box::from_raw(rust_boxed_logger_);
            }

        });

        rust_boxed_logger_ = nullptr;
    }

    // Brings overloaded Logv()s into scope so they're not hidden when we override
    // a subset of them.
    using Logger::Logv;

    virtual void Logv(const char* format, va_list ap) override {
        //Assume this is logged at the INFO level
        Logv(rocksdb::InfoLogLevel::INFO_LEVEL, format, ap);
    }

    virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) override {
        if (log_level < GetInfoLogLevel()) {
            return;
        }

        //Build this into a string.  No kidding this code is copied straight out of the RocksDb
        //source code.  Holy hard-coded buffer sizes!  The RocksDB code uses a hard-coded size
        //of 500; I made it double that, and if the buffer isn't big enough I just skip
        //logging.
        char msg[1024] = {0};
        int32_t n = vsnprintf(msg, sizeof(msg), format, ap);
        if (n > -1 && n < static_cast<int>(sizeof(msg))) {
             rust!(RustLogger_call_log [log_level: i32 as "int", msg: *const u8 as "char*", n: i32 as "int", rust_boxed_logger_: *mut CppLoggerWrapper as "void*"] {
                unsafe {
                    if let Some(logger) = rust_boxed_logger_.as_ref() {
                        logger.log(log_level, msg, n as usize);
                    }
                }
            });
        }
    }
private:
    void* rust_boxed_logger_;
};

}}

/// Trait which is implemented in Rust but which is converted into a RocksDb `Logger` class
/// implementation to capture RocksDB log events in Rust
///
/// Note that the methods on this trait will be called directly from the RocksDB threads thus this
/// must be completely thread safe and should not lock or block for any length of time or there
/// will be serious performance consequences.
pub trait RocksDbLogger: Send + Sync {
    /// Writes a header line to the log.  By default this is just logged as any other message at
    /// the INFO level.  The string is opaque and we can't make any assumptions about it's
    /// structure so it seems pretty useless to make this distinction.
    fn log_header(&self, header: &[u8]) {
        self.log(log::Level::Info, header);
    }

    /// Logs a C string directly from the C++ code.  This is NOT a Rust string type.  Most
    /// implementations should use the default impl of this method and instead implement `log_str`
    /// for better ergonomics
    fn log(&self, level: log::Level, msg: &[u8]) {
        //This message may or may not be valid UTF-8.  `from_utf8_lossy` will only allocate a new
        //string if `msg` is not a valid UTF-8 string already, in which case any invalid bytes will
        //be dropped
        let msg = String::from_utf8_lossy(msg);

        self.log_str(level, &msg);
    }

    fn log_str(&self, level: log::Level, msg: &str);
}

/// It's not convenient to pass pointers to `dyn RocksDbLogger` around to C code because `dyn` trait
/// pointers are fat (meaning two words not one).  It makes the code clearer to wrap that in a
/// struct which we will in turn box.  It's not different than `Box<Box<dyn RocksDbLogger>>`, except
/// for a bit less `WTF??`
pub struct CppLoggerWrapper<'a> {
    inner: Box<dyn RocksDbLogger + 'a>,
}

impl<'a> CppLoggerWrapper<'a> {
    pub(crate) fn new(logger: impl RocksDbLogger + 'a) -> Self {
        CppLoggerWrapper {
            inner: Box::new(logger),
        }
    }

    /// Presents a slightly more C-friendly interface on the `RocksDbLogger` trait, to make it easier
    /// to call from within C land.
    ///
    /// Decodes the level and calls either `log` or `log_header` on the wrapped implementation
    /// depending upon the log level
    unsafe fn log(&self, level: i32, msg: *const u8, length: usize) {
        let slice = std::slice::from_raw_parts(msg, length);

        if level == 5 {
            //InfoLogLevel::HEADER_LEVEL means log as a header line
            self.inner.log_header(slice);
        } else {
            //Convert the level to the `log` crate levels.  Note that the rocksdb code
            //hard-codeds an array based on the values of the enum so I don't feel so
            //bad hard coding here
            let level = match level {
                0 => log::Level::Debug,
                1 => log::Level::Info,
                2 => log::Level::Warn,
                3 | 4 => log::Level::Error,
                _ => log::Level::Debug,
            };
            self.inner.log(level, slice);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, DB};
    use std::sync::{Arc, Mutex};

    struct TestLogger {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl RocksDbLogger for TestLogger {
        fn log_str(&self, level: log::Level, msg: &str) {
            let mut messages = self.messages.lock().unwrap();
            messages.push(msg.to_owned());
        }
    }

    #[test]
    fn test_rust_logger() {
        let messages = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut options = Options::default();
        options.create_if_missing(true);

        let logger = TestLogger {
            messages: messages.clone(),
        };

        options.set_logger(log::Level::Debug, logger);

        let path = "_rust_logger_test";
        {
            let db = DB::open(&options, path).unwrap();
            db.put(b"k1", b"v1111").unwrap();
            db.put(b"k2", b"v1111").unwrap();
            db.put(b"k3", b"v1111").unwrap();
        }
        assert!(DB::destroy(&options, path).is_ok());

        //There should be messages logged as a result of all this
        let results: &Vec<String> = &messages.lock().unwrap();
        assert_ne!(Vec::<String>::new(), *results);
    }
}
