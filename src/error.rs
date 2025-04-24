use bincode::error::{EncodeError, DecodeError};
use serde::de;
use serde::ser;
use std::array::TryFromSliceError;
use std::fmt::Display;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::sync::PoisonError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    ParserError(String),
    InternalError(String),
    WriteConflict,
}

impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::InternalError(value.to_string())
    }
}

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::InternalError(value.to_string())
    }
}

impl std::error::Error for Error {}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::ParserError(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::ParserError(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::InternalError(err.to_string())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(err: PoisonError<T>) -> Self {
        Error::InternalError(err.to_string())
    }
}

impl From<TryFromSliceError> for Error {
    fn from(value: TryFromSliceError) -> Self {
        Error::InternalError(value.to_string())
    }
}

impl From<EncodeError> for Error {
    fn from(err: EncodeError) -> Self {
        Error::InternalError(err.to_string())
    }
}

impl From<DecodeError> for Error {
    fn from(err: DecodeError) -> Self {
        Error::InternalError(err.to_string())
    }
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InternalError(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InternalError(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ParserError(msg) => write!(f, "Parser error: {}", msg),
            Error::InternalError(msg) => write!(f, "Internal error: {}", msg),
            Error::WriteConflict => write!(f, "MVCC Write conflict, try transaction"),
        }
    }
}
