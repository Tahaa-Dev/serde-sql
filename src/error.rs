use std::{
    num::{IntErrorKind, ParseIntError},
    str::ParseBoolError,
};

#[derive(Debug)]
pub enum Error {
    InvalidCommand(String),
    InvalidType(String),
    UnexpectedToken(String, String),
    UnexpectedEOF,
    ParseFailure(String),
    InvalidMethod(String),
    InvalidParam(String),
    DuplicateIdent(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCommand(cmd) => {
                write!(
                    f,
                    "Invalid command: {}\nOnly `CREATE` statements are accepted",
                    cmd
                )
            }
            Self::InvalidType(ty) => write!(f, "Invalid type: {}", ty),
            Self::UnexpectedToken(untoken, token) => {
                write!(f, "Unexpected token: {}\nExpected: {}", untoken, token)
            }
            Self::UnexpectedEOF => f.write_str("Unexpected EOF"),
            Self::ParseFailure(s) => write!(f, "Failed to parse: {}", s),
            Self::InvalidMethod(s) => write!(f, "Invalid method: {}", s),
            Self::InvalidParam(s) => write!(f, "Invalid parameter: {}", s),
            Self::DuplicateIdent(s) => write!(f, "Duplicate identifier: {}", s),
        }
    }
}

impl<'a> From<nom::Err<nom::error::Error<&'a str>>> for Error {
    fn from(e: nom::Err<nom::error::Error<&'a str>>) -> Self {
        match e {
            nom::Err::Incomplete(_) => Error::UnexpectedEOF,

            nom::Err::Error(e) | nom::Err::Failure(e) => {
                let input = e.input.to_string();
                match e.code {
                    nom::error::ErrorKind::MultiSpace
                    | nom::error::ErrorKind::Space
                    | nom::error::ErrorKind::TakeWhile1 => Error::UnexpectedEOF,

                    nom::error::ErrorKind::Eof => Error::UnexpectedEOF,

                    nom::error::ErrorKind::Tag => {
                        Error::UnexpectedToken(input, "<KEYWORD>".into())
                    }

                    nom::error::ErrorKind::IsNot => Error::InvalidType(input),

                    nom::error::ErrorKind::Count => Error::ParseFailure(input),

                    nom::error::ErrorKind::ManyTill => {
                        Error::UnexpectedToken(input, "<INDEX METHOD>".into())
                    }

                    nom::error::ErrorKind::NoneOf => Error::InvalidParam(input),

                    nom::error::ErrorKind::AlphaNumeric
                    | nom::error::ErrorKind::Alpha => {
                        Error::UnexpectedToken(input, "<IDENTIFIER>".into())
                    }

                    nom::error::ErrorKind::Digit => {
                        Error::UnexpectedToken(input, "<NUMBER>".into())
                    }

                    nom::error::ErrorKind::OneOf => Error::DuplicateIdent(input),

                    _ => Error::ParseFailure(input),
                }
            }
        }
    }
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        match value.kind() {
            IntErrorKind::Empty => Error::ParseFailure("".into()),
            IntErrorKind::Zero => Error::ParseFailure("0".into()),
            _ => Error::ParseFailure("Invalid number".into()),
        }
    }
}

impl From<ParseBoolError> for Error {
    fn from(_: ParseBoolError) -> Self {
        Error::ParseFailure("Non bool".into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
