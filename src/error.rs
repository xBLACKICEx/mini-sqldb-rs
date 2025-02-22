use bincode::ErrorKind;
use std::sync::PoisonError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    ParserError(String),
    InternalError(String),
}

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

impl From<Box<ErrorKind>> for Error {
    fn from(err: Box<ErrorKind>) -> Self {
        Error::InternalError(err.to_string())
    }
}
