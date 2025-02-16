use crate::error::{Error, Result};
use std::fmt::Display;
use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),
    // Other types of string tokens, such as table names and column names
    Ident(String),
    // String type data
    String(String),
    // Numeric types, such as integers and floating-point numbers
    Number(String),

    OpenParen, // Left parenthesis (

    CloseParen, // Right parenthesis )

    Comma, // Comma ,

    Semicolon, // Semicolon ;

    Asterisk, // Asterisk *

    Plus, // Plus +

    Minus, // Minus -

    Slash, // Slash /
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Keyword(k) => write!(f, "{}", k),
            Token::Ident(s) => write!(f, "{}", s),
            Token::String(s) => write!(f, "{}", s),
            Token::Number(n) => write!(f, "{}", n),
            Token::OpenParen => write!(f, "("),
            Token::CloseParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
            Token::Semicolon => write!(f, ";"),
            Token::Asterisk => write!(f, "*"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Slash => write!(f, "/"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
}

impl Keyword {
    fn from_str(ident: &str) -> Option<Self> {
        match ident.to_uppercase().as_str() {
            "CREATE" => Some(Keyword::Create),
            "TABLE" => Some(Keyword::Table),
            "INT" => Some(Keyword::Int),
            "INTEGER" => Some(Keyword::Integer),
            "BOOLEAN" => Some(Keyword::Boolean),
            "BOOL" => Some(Keyword::Bool),
            "STRING" => Some(Keyword::String),
            "TEXT" => Some(Keyword::Text),
            "VARCHAR" => Some(Keyword::Varchar),
            "FLOAT" => Some(Keyword::Float),
            "DOUBLE" => Some(Keyword::Double),
            "SELECT" => Some(Keyword::Select),
            "FROM" => Some(Keyword::From),
            "INSERT" => Some(Keyword::Insert),
            "INTO" => Some(Keyword::Into),
            "VALUES" => Some(Keyword::Values),
            "TRUE" => Some(Keyword::True),
            "FALSE" => Some(Keyword::False),
            "DEFAULT" => Some(Keyword::Default),
            "NOT" => Some(Keyword::Not),
            "NULL" => Some(Keyword::Null),
            "PRIMARY" => Some(Keyword::Primary),
            "KEY" => Some(Keyword::Key),
            _ => None,
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let keyword = match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
        };
        write!(f, "{}", keyword)
    }
}

/// Lexical Analyzer Lexer Definition
/// Currently supported SQL syntax
/// 1. Create Table
/// -------------------------------------
/// CREATE TABLE table_name (
///     [ column_name data_type [ column_constraint [...] ] ]
///     [, ... ]
///    );
///
///    where data_type is:
///     - BOOLEAN(BOOL): true | false
///     - FLOAT(DOUBLE)
///     - INTEGER(INT)
///     - STRING(TEXT, VARCHAR)
///
///    where column_constraint is:
///    [ NOT NULL | NULL | DEFAULT expr ]
///
/// 2. Insert Into
/// -------------------------------------
/// INSERT INTO table_name
/// [ ( column_name [, ...] ) ]
/// values ( expr [, ...] );
///
/// 3. Select * From
/// -------------------------------------
/// SELECT * FROM table_name;
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    // Remove whitespace characters
    // eg. select *       from        t;
    fn erase_whitespace(&mut self) {
        self.next_while(char::is_whitespace);
    }

    // If the condition is met, jump to the next character and return the character
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?; // Return the current character if the condition is met
        self.iter.next()
    }

    // Determine whether the current character meets the condition, and if it does, jump to the next character
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }

        Some(value).filter(|v| !v.is_empty())
    }

    // Only jump to the next if it is a Token type, and return Token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|&c| predicate(c))?;
        self.iter.next();
        Some(token)
    }

    // Scan to get the next Token
    fn scan(&mut self) -> Result<Option<Token>> {
        // Remove whitespace characters in the string
        self.erase_whitespace();
        // Determine based on the first character
        match self.iter.peek() {
            Some('\'') => self.scan_string(), // Scan string
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()), // Scan number
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()), // Scan Ident type
            Some(_) => Ok(self.scan_symbol()), // Scan symbol
            None => Ok(None),
        }
    }

    // Scan string
    fn scan_string(&mut self) -> Result<Option<Token>> {
        // Determine whether it starts with a single quote
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }

        let mut val = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => val.push(c),
                None => {
                    return Err(Error::ParserError(
                        "[Lexer] Unexpected end of string".to_string(),
                    ))
                }
            }
        }

        Ok(Some(Token::String(val)))
    }

    // Scan number
    fn scan_number(&mut self) -> Option<Token> {
        // Scan a part first
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        // If there is a decimal point in the middle, it means it is a floating point number
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            // Scan the part after the decimal point
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }

        Some(Token::Number(num))
    }

    // Scan Ident types such as table names, column names, or keywords such as CREATE, TABLE
    fn scan_ident(&mut self) -> Option<Token> {
        let mut val = self.next_if(char::is_alphabetic)?.to_string();

        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            val.push(c);
        }

        Keyword::from_str(&val).map_or(Some(Token::Ident(val)), |k| Some(Token::Keyword(k)))
    }

    // Scan symbol
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '*' => Some(Token::Asterisk),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            _ => None,
        })
    }
}

// Custom iterator that returns Token
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self.iter.peek().map(|c| {
                Err(Error::ParserError(format!(
                    "[Lexer] Unexpected character: '{}'",
                    c
                )))
            }),
            Err(err) => Some(Err(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        error::Error,
        error::Result,
        sql::parser::lexer::{Keyword, Token},
    };

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens1 = Lexer::new(
            "
                    CREATE table tbl
                    (
                        a int primary key,
                        id2 integer
                    );
                    ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("a".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_create_table_more() -> Result<()> {
        let tokens2 = Lexer::new(
            "CREATE table tbl
                    (
                        id1 int primary key,
                        id2 integer,
                        c1 bool null,
                        c2 boolean not null,
                        c3 float null,
                        c4 double,
                        c5 string,
                        c6 text,
                        c7 varchar default 'foo',
                        c8 int default 100,
                        c9 integer
                    );
                    ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                // id1 int primary key,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                // id2 integer,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::Comma,
                // c1 bool null,
                Token::Ident("c1".to_string()),
                Token::Keyword(Keyword::Bool),
                Token::Keyword(Keyword::Null),
                Token::Comma,
                // c2 boolean not null,
                Token::Ident("c2".to_string()),
                Token::Keyword(Keyword::Boolean),
                Token::Keyword(Keyword::Not),
                Token::Keyword(Keyword::Null),
                Token::Comma,
                // c3 float null,
                Token::Ident("c3".to_string()),
                Token::Keyword(Keyword::Float),
                Token::Keyword(Keyword::Null),
                Token::Comma,
                // c4 double,
                Token::Ident("c4".to_string()),
                Token::Keyword(Keyword::Double),
                Token::Comma,
                // c5 string,
                Token::Ident("c5".to_string()),
                Token::Keyword(Keyword::String),
                Token::Comma,
                // c6 text,
                Token::Ident("c6".to_string()),
                Token::Keyword(Keyword::Text),
                Token::Comma,
                // c7 varchar default 'foo',
                Token::Ident("c7".to_string()),
                Token::Keyword(Keyword::Varchar),
                Token::Keyword(Keyword::Default),
                Token::String("foo".to_string()),
                Token::Comma,
                // c8 int default 100,
                Token::Ident("c8".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Default),
                Token::Number("100".to_string()),
                Token::Comma,
                // c9 integer
                Token::Ident("c9".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_number() -> Result<()> {
        let tokens = Lexer::new("12345 67.89")
            .peekable()
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            tokens,
            vec![
                Token::Number("12345".to_string()),
                Token::Number("67.89".to_string())
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_string_literal() -> Result<()> {
        let tokens = Lexer::new("'hello world'")
            .peekable()
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(tokens, vec![Token::String("hello world".to_string())]);
        Ok(())
    }

    #[test]
    fn test_lexer_error_unclosed_string() {
        let mut lex = Lexer::new("'unclosed string");
        let token = lex.next();
        match token {
            Some(Err(Error::ParserError(msg))) => {
                assert!(msg.contains("Unexpected end of string"));
            }
            _ => panic!("Expected an error for unclosed string"),
        }
    }

    #[test]
    fn test_lexer_unexpected_symbol() {
        // '@' is not a supported symbol, so it should return an error.
        let mut lex = Lexer::new("@");
        let token = lex.next();
        match token {
            Some(Err(Error::ParserError(msg))) => {
                assert!(msg.contains("Unexpected character"));
            }
            _ => panic!("Expected an error for unsupported symbol"),
        }
    }

    #[test]
    fn test_lexer_insert_into() -> Result<()> {
        let tokens1 = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_select_from() -> Result<()> {
        let sql = "SELECT * FROM users";
        let tokens = Lexer::new(sql).peekable().collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("users".to_string()),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_float_number() -> Result<()> {
        let sql = "3.14 0.5";
        let tokens = Lexer::new(sql).peekable().collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Number("3.14".to_string()),
                Token::Number("0.5".to_string()),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_operators() -> Result<()> {
        let sql = "+ - * /";
        let tokens = Lexer::new(sql).peekable().collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![Token::Plus, Token::Minus, Token::Asterisk, Token::Slash,]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_mixed_case_keywords() -> Result<()> {
        let sql = "SeLeCt * FrOm users";
        let tokens = Lexer::new(sql).peekable().collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("users".to_string()),
            ]
        );

        Ok(())
    }
}
