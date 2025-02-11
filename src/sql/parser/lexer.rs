use crate::error::{Error, Result};
use std::{iter::Peekable, str::Chars};

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

/// Lexer Definition
///
/// Supported SQL syntax:
///
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

    pub fn scan(&mut self) -> Result<Option<Token>> {
        unimplemented!()
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

mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        error::Error,
        error::Result,
        sql::parser::lexer::{Keyword, Token},
    };

    #[test]
    fn test_lexer_crate_table() -> Result<()> {
        let tokens1 = Lexer::new(
            "
            CREATE table tbl
            (
                id1 int primary key,
                id2 integer
            );
            ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        println!("{:?}", tokens1);

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
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
}
