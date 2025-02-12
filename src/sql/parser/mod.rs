use super::types::DataType;
use crate::error::{Error, Result};
use ast::Column;
use lexer::{Keyword, Lexer, Token};
use std::iter::Peekable;

mod ast;
mod lexer;

pub struct Parser<'a> {
    lexer: Peekable<lexer::Lexer<'a>>,
}

// Parser definition
impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    // Parse to get the abstract syntax tree
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        // Expect a semicolon at the end of the SQL statement
        self.next_expect(Token::Semicolon)?;
        // There should be no other symbols after the semicolon
        if let Some(token) = self.peek()? {
            return Err(Error::ParserError(format!(
                "[Parser] Unexpected token {token}"
            )));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(_) => Err(Error::ParserError("[Parser] Unexpected token".to_string())),
            None => Err(Error::ParserError(
                "[Parser] Unexpected end of input".to_string(),
            )),
        }
    }

    // Parse Create DDL statements
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::ParserError(format!(
                    "[Parser] Unexpected token, expected TABLE but got {token}"
                ))),
            },

            _ => Err(Error::ParserError(
                "[Parser] Unexpected end of input".to_string(),
            )),
        }
    }

    // Parse Create Table statement
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // Expect the Table name
        let table_name = self.next_ident()?;
        self.next_expect(Token::OpenParen)?;
        // There should be parentheses after the table name
        let mut columns = vec![];
        loop {
            columns.push(self.parse_ddl_column()?);
            // If there is no comma, the column parsing is complete, break out of the loop
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable {
            name: table_name,
            columns,
        })
    }

    // Parse column information
    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            data_type: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                    DataType::Integer
                }
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::Boolean) | Token::Keyword(Keyword::Bool) => {
                    DataType::Boolean
                }
                token => {
                    return Err(Error::ParserError(format!(
                        "[Parser] Unexpected token {token}"
                    )))
                }
            },
            nullable: None,
            default: None,
        };

        // Parse the default value of the column and whether it can be empty
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                keyword => {
                    return Err(Error::ParserError(format!(
                        "[Parser] Unexpected keyword {keyword}"
                    )))
                }
            }
        }

        Ok(column)
    }

    // Parse expressions
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            token => {
                return Err(Error::ParserError(format!(
                    "[Parser] Unexpected expression token {token}"
                )))
            }
        })
    }

    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;

        // Expect the table name
        let table_name = self.next_ident()?;
        Ok(ast::Statement::Select { table_name })
    }

    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;
        let table_name = self.next_ident()?;

        let mut columns = None;
        if self.next_if_token(Token::OpenParen).is_some() {
            columns = Some(self.parse_insert_columns()?);
        }

        self.next_expect(Token::Keyword(Keyword::Values))?;

        // insert into tbl(a, b,c) values (1, 2, 3), (4, 5, 6);
        let values = self.parse_values()?;

        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }

    fn parse_values(&mut self) -> Result<Vec<Vec<ast::Expression>>> {
        let mut values = vec![];
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = vec![];
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => continue,
                    token => {
                        return Err(Error::ParserError(format!(
                            "[Parser] Unexpected token {token}"
                        )))
                    }
                }
            }
            values.push(exprs);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(values)
    }
    
    fn parse_insert_columns(&mut self) -> Result<Vec<String>> {
        let mut columns = vec![];
        loop {
            match self.next()? {
                Token::Ident(s) => columns.push(s),
                Token::Comma => continue,
                Token::CloseParen => break,
                token => {
                    return Err(Error::ParserError(format!(
                        "[Parser] Unexpected token {token}"
                    )))
                }
            }
        }
        Ok(columns)
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer.next().unwrap_or_else(|| {
            Err(Error::ParserError(
                "[Parser] Unexpected end of input".to_string(),
            ))
        })
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(s) => Ok(s),
            token => Err(Error::ParserError(format!(
                "[Parser] Expected identifier, got token {token}"
            ))),
        }
    }

    fn next_expect(&mut self, expected: Token) -> Result<()> {
        match self.next()? {
            token if token == expected => Ok(()),
            token => Err(Error::ParserError(format!(
                "[Parser] Expected token {expected}, got {token}"
            ))),
        }
    }

    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(predicate)?;
        self.next().ok()
    }

    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|token| matches!(token, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, expected: Token) -> Option<Token> {
        self.next_if(|token| *token == expected)
    }
}

#[cfg(test)]
mod tests {
    // use super::ast;
    // use super::types::DataType;
    // use super::Parser;
    use super::*;

    #[test]
    fn test_parser_create_table_basic() -> Result<()> {
        let sql = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let _stmt = Parser::new(sql).parse()?;
        Ok(())
    }

    #[test]
    fn test_parser_create_table_whitespace() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt1 = Parser::new(sql1).parse()?;

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1, stmt2);
        Ok(())
    }

    #[test]
    fn test_parser_create_table_missing_semicolon() {
        let sql = "
            create            table tbl1 (
                a int default     100,
                b float not null     ,
                c varchar      null,
                d       bool default        true
            )
        ";
        let stmt = Parser::new(sql).parse();
        assert!(stmt.is_err());
    }

    #[test]
    fn test_select() {
        let sql = "SELECT * FROM my_table;";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse().expect("Failed to parse SELECT");
        match stmt {
            ast::Statement::Select { table_name } => {
                assert_eq!(table_name, "my_table");
            }
            _ => panic!("Statement should be Select"),
        }
    }

    #[test]
    fn test_insert_with_columns() {
        let sql = "INSERT INTO my_table (id, name) VALUES (1, 'Alice'), (2, 'Bob');";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse().expect("Failed to parse INSERT with columns");
        match stmt {
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => {
                assert_eq!(table_name, "my_table");
                let cols = columns.expect("Expected columns for INSERT");
                assert_eq!(cols, vec!["id", "name"]);
                assert_eq!(values.len(), 2);

                // We assume expressions are represented via ast::Expression.
                // Here we just check that each row contains the correct number of expressions.
                for row in values {
                    assert_eq!(row.len(), 2);
                }
            }
            _ => panic!("Statement should be Insert"),
        }
    }

    #[test]
    fn test_insert_without_columns() {
        let sql = "INSERT INTO my_table VALUES (1, 'Alice');";
        let mut parser = Parser::new(sql);
        let stmt = parser
            .parse()
            .expect("Failed to parse INSERT without columns");
        match stmt {
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => {
                assert_eq!(table_name, "my_table");
                assert!(columns.is_none(), "Expected no columns for INSERT");
                assert_eq!(values.len(), 1);
            }
            _ => panic!("Statement should be Insert"),
        }
    }

    #[test]
    fn test_missing_semicolon_error() {
        let sql = "SELECT * FROM my_table";
        let mut parser = Parser::new(sql);
        let res = parser.parse();
        assert!(res.is_err(), "Expected error due to missing semicolon");
    }

    #[test]
    fn test_unexpected_token_error() {
        let sql = "RANDOM TOKEN;";
        let mut parser = Parser::new(sql);
        let res = parser.parse();
        assert!(res.is_err(), "Expected error for unexpected token");
    }
}
