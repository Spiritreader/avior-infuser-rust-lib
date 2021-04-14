use chrono;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::prelude::*;

pub struct Logger {
    buffer: Vec<String>,
    kopfer: String,
}

pub enum Mode {
    Append,
    Overwrite,
}

pub trait Log {
    fn new(kopferino: &str) -> Self;
    fn add(&mut self, message: &str);
    fn clear(&mut self);
    fn flush(&mut self, path: &str, mode: Mode) -> Result<(), Box<dyn Error>>;
}

impl Logger {}

impl Log for Logger {
    /// Creates a new Logger instance
    fn new(kopferino: &str) -> Self {
        Logger {
            buffer: Vec::new(),
            kopfer: kopferino.to_owned(),
        }
    }

    /// Appends a line to the log buffer
    fn add(&mut self, message: &str) {
        println!("{}", message);
        self.buffer.push(message.into())
    }

    /// Clears the logging queue
    fn clear(&mut self) {
        self.buffer.clear()
    }

    /// Flushes the log contents to disk
    ///
    /// ### Parameters:
    /// - path: a valid OS filepath including the file extension
    /// - mode: a mode string being either
    fn flush(&mut self, path: &str, mode: Mode) -> Result<(), Box<dyn Error>> {
        let append = match mode {
            Mode::Append => true,
            Mode::Overwrite => false,
        };
        let mut logfile = OpenOptions::new().write(true).append(append).create(true).open(path)?;
        writeln!(
            logfile,
            "{}",
            chrono::offset::Local::now().format("%Y-%m-%d %H:%M:%S %z").to_string()
        )?;
        writeln!(logfile, "{}", self.kopfer)?;
        for line in self.buffer.iter() {
            writeln!(logfile, "{}", line)?;
        }
        writeln!(logfile, "")?;
        self.clear();
        Ok(())
    }
}
