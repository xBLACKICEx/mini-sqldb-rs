use std::{collections::BTreeMap, iter::Peekable};

use ast::{Column, Expression, OrderDirection};
use lexer::{Keyword, Lexer, Token};

use crate::error::{Error, Result};

use super::types::DataType;


pub(super) mod ast;
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

    /// Parse to get the abstract syntax tree
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
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
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
            primary_key: false,
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
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.primary_key = true;
                }
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

        Ok(ast::Statement::Select {
            table_name,
            where_clause: self.parse_where_clause()?,
            order_by: self.parse_order_clause()?,
        })
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
            let mut express = vec![];
            loop {
                express.push(self.parse_expression()?);
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
            values.push(express);
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

    fn parse_update(&mut self) -> std::result::Result<ast::Statement, Error> {
        self.next_expect(Token::Keyword(Keyword::Update))?;
        let table_name = self.next_ident()?;
        self.next_expect(Token::Keyword(Keyword::Set))?;
        let mut columns = BTreeMap::new();
        loop {
            let column = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let expr = self.parse_expression()?;
            if columns.contains_key(&column) {
                return Err(Error::ParserError(format!(
                    "[Parser] Duplicate column name {column}"
                )));
            }
            columns.insert(column, expr);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Update {
            table_name,
            columns,
            where_clause: self.parse_where_clause()?,
        })
    }

    fn parse_delete(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Delete))?;
        self.next_expect(Token::Keyword(Keyword::From))?;

        let table_name = self.next_ident()?;
        let where_clause = self.parse_where_clause()?;

        Ok(ast::Statement::Delete {
            table_name,
            where_clause,
        })
    }

    fn parse_where_clause(&mut self) -> Result<Option<(String, Expression)>> {
        if self.next_if_token(Token::Keyword(Keyword::Where)).is_some() {
            let column = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let value = self.parse_expression()?;
            Ok(Some((column, value)))
        } else {
            Ok(None)
        }
    }

    fn parse_order_clause(&mut self) -> Result<Vec<(String, OrderDirection)>> {
        let mut orders = vec![];

        if self.next_if_token(Token::Keyword(Keyword::Order)).is_none() {
            return Ok(orders);
        }
        self.next_expect(Token::Keyword(Keyword::By))?;

        loop {
            let col = self.next_ident()?;
            let ord = match self.next_if(|t| {
                matches!(t, Token::Keyword(Keyword::Asc) | Token::Keyword(Keyword::Desc))
            }) {
                Some(Token::Keyword(Keyword::Asc)) => OrderDirection::Asc,
                Some(Token::Keyword(Keyword::Desc)) => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };
            orders.push((col, ord));

            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(orders)
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
                "[Parser] Expected token {expected} but got {token}"
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
    use crate::sql::parser::ast::{Consts, Statement};

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

    macro_rules! parse_eq {
        ($sql:expr, $expected:expr) => {
            assert_eq!(Parser::new($sql).parse().unwrap(), $expected);
        };
    }

    #[test]
    fn test_select() {
        use OrderDirection::*;

        parse_eq!(
            "SELECT * FROM my_table;",
            ast::Statement::Select {
                table_name: "my_table".to_string(),
                where_clause: None,
                order_by: vec![],
            }
        );

        parse_eq!(
            "SELECT * FROM my_table ORDER by a, b asc, c desc;",
            ast::Statement::Select {
                table_name: "my_table".to_string(),
                where_clause: None,
                order_by: vec![
                    ("a".to_string(), Asc),
                    ("b".to_string(), Asc),
                    ("c".to_string(), Desc)
                ],
            }
        );
    }

    #[test]
    fn test_select_with_where() {
        parse_eq!(
            "SELECT * FROM my_table WHERE id = 42;",
            ast::Statement::Select {
                table_name: "my_table".to_string(),
                where_clause: Some(("id".to_string(), Expression::Consts(Consts::Integer(42)))),
                order_by: Vec::new(),
            }
        );
    }

    #[test]
    fn test_insert_with_columns() {
        let mut vals = Vec::new();
        // 这里只关注行数和列数，也可以把具体 Expression 展开
        vals.push(vec![
            Expression::Consts(Consts::Integer(1)),
            Expression::Consts(Consts::String("Alice".into())),
        ]);
        vals.push(vec![
            Expression::Consts(Consts::Integer(2)),
            Expression::Consts(Consts::String("Bob".into())),
        ]);
        parse_eq!(
            "INSERT INTO my_table (id, name) VALUES (1, 'Alice'), (2, 'Bob');",
            Statement::Insert {
                table_name: "my_table".to_string(),
                columns: Some(vec!["id".to_string(), "name".to_string()]),
                values: vals,
            }
        );
    }

    #[test]
    fn test_insert_without_columns() {
        parse_eq!(
            "INSERT INTO my_table VALUES (1, 'Alice');",
            Statement::Insert {
                table_name: "my_table".to_string(),
                columns: None,
                values: vec![vec![
                    Expression::Consts(Consts::Integer(1)),
                    Expression::Consts(Consts::String("Alice".into())),
                ]],
            }
        );
    }

    #[test]
    fn test_missing_semicolon_error() {
        assert!(Parser::new("SELECT * FROM my_table").parse().is_err());
    }

    #[test]
    fn test_unexpected_token_error() {
        assert!(Parser::new("RANDOM TOKEN;").parse().is_err());
    }

    #[test]
    fn test_update() {
        let mut cols = BTreeMap::new();
        cols.insert(
            "name".to_string(),
            Expression::Consts(Consts::String("Alice".into())),
        );
        cols.insert("age".to_string(), Expression::Consts(Consts::Integer(30)));
        parse_eq!(
            "UPDATE my_table SET name = 'Alice', age = 30 WHERE id = 1;",
            Statement::Update {
                table_name: "my_table".to_string(),
                columns: cols,
                where_clause: Some(("id".to_string(), Expression::Consts(Consts::Integer(1)))),
            }
        );
    }

    #[test]
    fn test_update_failure_scenarios() {
        // Test duplicate column in SET clause
        let sql = "UPDATE my_table SET name = 'Alice', name = 'Bob' WHERE id = 1;";
        let mut parser = Parser::new(sql);
        let result = parser.parse();
        assert!(result.is_err(), "Should fail on duplicate column name");
        if let Err(Error::ParserError(msg)) = result {
            assert!(msg.contains("Duplicate column name"));
        } else {
            panic!("Expected ParserError with duplicate column message");
        }

        // Test invalid syntax (missing SET keyword)
        let sql = "UPDATE my_table name = 'Alice' WHERE id = 1;";
        let mut parser = Parser::new(sql);
        let result = parser.parse();
        assert!(result.is_err(), "Should fail when SET keyword is missing");

        // Test invalid expression in SET clause
        let sql = "UPDATE my_table SET name = WHERE id = 1;";
        let mut parser = Parser::new(sql);
        let result = parser.parse();
        assert!(result.is_err(), "Should fail with invalid expression");

        // Test invalid WHERE clause
        let sql = "UPDATE my_table SET name = 'Alice' WHERE;";
        let mut parser = Parser::new(sql);
        let result = parser.parse();
        assert!(result.is_err(), "Should fail with incomplete WHERE clause");
    }
}
