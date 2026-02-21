#[derive(Debug)]
pub enum LexError {
    InvalidCommand(String),
    UnclosedParentheses,
    InvalidType(String),
    UnexpectedToken(String, String),
    UnexpectedEOF,
}

impl std::error::Error for LexError {}

impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCommand(cmd) => write!(
                f,
                "Invalid command: {}\nOnly `CREATE` statements are accepted",
                cmd
            ),
            Self::UnclosedParentheses => {
                write!(f, "Invalid `CREATE` statement\nFound unclosed parentheses")
            }
            Self::InvalidType(ty) => write!(f, "Invalid type: {}", ty),
            Self::UnexpectedToken(untoken, token) => {
                write!(f, "Unexpected token: {}\nExpected: {}", untoken, token)
            }
            Self::UnexpectedEOF => write!(f, "Unexpected EOF"),
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, LexError>;
