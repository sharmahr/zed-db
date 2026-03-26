"""
SQL Parser

Converts a token stream into an Abstract Syntax Tree (AST).
"""

from typing import List, Optional
from zed.sql.tokenizer import Tokenizer, Token, TokenType
from zed.sql.ast import (
    Statement,
    CreateTableStatement,
    InsertStatement,
    SelectStatement,
    DeleteStatement,
    UpdateStatement,
    BeginStatement,
    CommitStatement,
    RollbackStatement,
    ColumnDef,
    ColumnRef,
    TableRef,
    Literal,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    JoinClause,
    Expression,
)


class ParseError(Exception):
    """Raised when parsing fails."""
    pass


class Parser:
    """
    Recursive descent SQL parser.
    
    Usage:
        parser = Parser("SELECT * FROM users WHERE id = 1")
        statements = parser.parse()
    """
    
    def __init__(self, sql: str):
        self.tokenizer = Tokenizer(sql)
        self.tokens: List[Token] = []
        self.pos = 0
        self.current: Optional[Token] = None
    
    def _tokenize(self):
        """Tokenize the SQL and initialize token stream."""
        self.tokens = self.tokenizer.tokenize()
        self.pos = 0
        self.current = self.tokens[0] if self.tokens else None
    
    def _advance(self) -> Token:
        """Advance to next token and return the previous one."""
        prev = self.current
        self.pos += 1
        if self.pos < len(self.tokens):
            self.current = self.tokens[self.pos]
        else:
            self.current = self.tokens[-1]  # Stay on EOF
        return prev
    
    def _peek(self, offset: int = 0) -> Optional[Token]:
        """Peek at token at current position + offset."""
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return None
    
    def _check(self, *types: TokenType) -> bool:
        """Check if current token is one of the given types."""
        if self.current is None:
            return False
        return self.current.type in types
    
    def _match(self, *types: TokenType) -> bool:
        """If current token matches, advance and return True."""
        if self._check(*types):
            self._advance()
            return True
        return False
    
    def _consume(self, type: TokenType, message: str) -> Token:
        """Consume token of given type or raise error."""
        if self._check(type):
            return self._advance()
        raise ParseError(f"{message}. Got {self.current}")
    
    def _synchronize(self):
        """Skip tokens until we're at a statement boundary."""
        self._advance()
        while self.current and self.current.type != TokenType.EOF:
            if self._check(
                TokenType.CREATE, TokenType.INSERT, TokenType.SELECT,
                TokenType.BEGIN, TokenType.COMMIT, TokenType.ROLLBACK
            ):
                return
            self._advance()
    
    # =========================================================================
    # Main Parse Entry
    # =========================================================================
    
    def parse(self) -> List[Statement]:
        """Parse the SQL and return list of statements."""
        self._tokenize()
        statements: List[Statement] = []
        
        while not self._check(TokenType.EOF):
            try:
                stmt = self._parse_statement()
                if stmt:
                    statements.append(stmt)
            except ParseError:
                self._synchronize()
        
        return statements
    
    def _parse_statement(self) -> Optional[Statement]:
        """Parse a single statement."""
        if self._check(TokenType.CREATE):
            return self._parse_create_table()
        if self._check(TokenType.INSERT):
            return self._parse_insert()
        if self._check(TokenType.DELETE):
            return self._parse_delete()
        if self._check(TokenType.UPDATE):
            return self._parse_update()
        if self._check(TokenType.SELECT):
            return self._parse_select()
        if self._check(TokenType.BEGIN):
            self._advance()
            self._match(TokenType.SEMICOLON)
            return BeginStatement()
        if self._check(TokenType.COMMIT):
            self._advance()
            self._match(TokenType.SEMICOLON)
            return CommitStatement()
        if self._check(TokenType.ROLLBACK):
            self._advance()
            self._match(TokenType.SEMICOLON)
            return RollbackStatement()
        
        # Skip unknown tokens
        self._advance()
        return None
    
    # =========================================================================
    # CREATE TABLE
    # =========================================================================
    
    def _parse_create_table(self) -> CreateTableStatement:
        """Parse CREATE TABLE statement."""
        self._consume(TokenType.CREATE, "Expected CREATE")
        self._consume(TokenType.TABLE, "Expected TABLE")
        
        # Table name
        name_token = self._consume(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # Column definitions
        self._consume(TokenType.LPAREN, "Expected '(' after table name")
        
        columns: List[ColumnDef] = []
        while not self._check(TokenType.RPAREN) and not self._check(TokenType.EOF):
            columns.append(self._parse_column_def())
            
            if not self._match(TokenType.COMMA):
                break
        
        self._consume(TokenType.RPAREN, "Expected ')' after column definitions")
        self._match(TokenType.SEMICOLON)
        
        return CreateTableStatement(table_name=table_name, columns=columns)
    
    def _parse_column_def(self) -> ColumnDef:
        """Parse a column definition."""
        # Column name
        name_token = self._consume(TokenType.IDENTIFIER, "Expected column name")
        name = name_token.value
        
        # Data type
        dtype = self._parse_type()
        
        # Modifiers
        nullable = True
        primary_key = False
        default = None
        
        while True:
            if self._match(TokenType.PRIMARY):
                self._consume(TokenType.KEY, "Expected KEY after PRIMARY")
                primary_key = True
                nullable = False
            elif self._match(TokenType.NOT):
                self._consume(TokenType.NULL, "Expected NULL after NOT")
                nullable = False
            elif self._match(TokenType.DEFAULT):
                default = self._parse_expression()
            else:
                break
        
        return ColumnDef(
            name=name,
            dtype=dtype,
            nullable=nullable,
            primary_key=primary_key,
            default=default
        )
    
    def _parse_type(self) -> str:
        """Parse a data type."""
        if self._match(TokenType.INT):
            return "INT"
        if self._match(TokenType.TEXT):
            return "TEXT"
        if self._match(TokenType.REAL):
            return "REAL"
        if self._match(TokenType.BOOL):
            return "BOOL"
        
        # Default to INT for unknown types
        if self.current:
            self._advance()  # Skip unknown type
        return "INT"
    
    # =========================================================================
    # INSERT
    # =========================================================================
    
    def _parse_insert(self) -> InsertStatement:
        """Parse INSERT INTO statement."""
        self._consume(TokenType.INSERT, "Expected INSERT")
        self._consume(TokenType.INTO, "Expected INTO")
        
        # Table name
        name_token = self._consume(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # Optional column list
        columns: Optional[List[str]] = None
        if self._match(TokenType.LPAREN):
            columns = []
            while not self._check(TokenType.RPAREN):
                col_token = self._consume(TokenType.IDENTIFIER, "Expected column name")
                columns.append(col_token.value)
                if not self._match(TokenType.COMMA):
                    break
            self._consume(TokenType.RPAREN, "Expected ')' after column list")
        
        # VALUES
        self._consume(TokenType.VALUES, "Expected VALUES")
        
        # Value rows
        values: List[List[Expression]] = []
        
        # First row
        values.append(self._parse_value_row())
        
        # Additional rows
        while self._match(TokenType.COMMA):
            values.append(self._parse_value_row())
        
        self._match(TokenType.SEMICOLON)
        
        return InsertStatement(
            table_name=table_name,
            columns=columns,
            values=values
        )
    
    def _parse_value_row(self) -> List[Expression]:
        """Parse a VALUES row: (expr, expr, ...)."""
        self._consume(TokenType.LPAREN, "Expected '(' in VALUES")
        
        row: List[Expression] = []
        
        while not self._check(TokenType.RPAREN):
            row.append(self._parse_expression())
            if not self._match(TokenType.COMMA):
                break
        
        self._consume(TokenType.RPAREN, "Expected ')' after VALUES row")
        return row
    
    # =========================================================================
    # DELETE
    # =========================================================================
    
    def _parse_delete(self) -> DeleteStatement:
        """Parse DELETE FROM statement."""
        self._consume(TokenType.DELETE, "Expected DELETE")
        self._consume(TokenType.FROM, "Expected FROM")
        
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()
        
        self._match(TokenType.SEMICOLON)
        return DeleteStatement(table_name=table_name, where=where)
    
    # =========================================================================
    # UPDATE
    # =========================================================================
    
    def _parse_update(self) -> UpdateStatement:
        """Parse UPDATE statement."""
        self._consume(TokenType.UPDATE, "Expected UPDATE")
        
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        self._consume(TokenType.SET, "Expected SET")
        
        # Parse set_values: col = expr, col = expr, ...
        set_values: Dict[str, Expression] = {}
        while True:
            col_name = self._consume(TokenType.IDENTIFIER, "Expected column name").value
            self._consume(TokenType.EQUALS, "Expected =")
            value_expr = self._parse_expression()
            set_values[col_name] = value_expr
            
            if not self._match(TokenType.COMMA):
                break
        
        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()
        
        self._match(TokenType.SEMICOLON)
        return UpdateStatement(table_name=table_name, set_values=set_values, where=where)
    
    # =========================================================================
    # SELECT
    # =========================================================================
    
    def _parse_select(self) -> SelectStatement:
        """Parse SELECT statement."""
        self._consume(TokenType.SELECT, "Expected SELECT")
        
        # DISTINCT
        distinct = self._match(TokenType.DISTINCT)
        
        # Columns (or *)
        columns: List[Expression] = []
        
        if self._check(TokenType.STAR):
            self._advance()
            columns.append(ColumnRef("*"))
        else:
            while True:
                columns.append(self._parse_expression())
                if not self._match(TokenType.COMMA):
                    break
        
        # FROM
        tables: List[TableRef] = []
        joins: List[JoinClause] = []
        
        if self._match(TokenType.FROM):
            tables.append(self._parse_table_ref())
            
            # Joins
            while True:
                join_type = self._parse_join_type()
                if join_type is None:
                    break
                
                table = self._parse_table_ref()
                on = None
                
                if join_type != "CROSS" and self._match(TokenType.ON):
                    on = self._parse_expression()
                
                joins.append(JoinClause(join_type=join_type, table=table, on=on))
        
        # WHERE
        where: Optional[Expression] = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()
        
        # GROUP BY
        group_by: List[Expression] = []
        if self._match(TokenType.GROUP):
            self._consume(TokenType.BY, "Expected BY after GROUP")
            while True:
                group_by.append(self._parse_expression())
                if not self._match(TokenType.COMMA):
                    break
        
        # HAVING
        having: Optional[Expression] = None
        if self._match(TokenType.HAVING):
            having = self._parse_expression()
        
        # ORDER BY
        order_by: List[tuple] = []
        if self._match(TokenType.ORDER):
            self._consume(TokenType.BY, "Expected BY after ORDER")
            while True:
                expr = self._parse_expression()
                direction = "ASC"
                if self._match(TokenType.ASC):
                    direction = "ASC"
                elif self._match(TokenType.DESC):
                    direction = "DESC"
                order_by.append((expr, direction))
                if not self._match(TokenType.COMMA):
                    break
        
        # LIMIT / OFFSET
        limit: Optional[int] = None
        offset: Optional[int] = None
        
        # Parse LIMIT
        if self._match(TokenType.LIMIT):
            limit_token = self._consume(TokenType.NUMBER, "Expected number after LIMIT")
            limit = int(limit_token.value)
            
            # Optional OFFSET after LIMIT
            if self._match(TokenType.OFFSET):
                offset_token = self._consume(TokenType.NUMBER, "Expected number after OFFSET")
                offset = int(offset_token.value)
        
        # Parse OFFSET (standalone, before LIMIT)
        elif self._match(TokenType.OFFSET):
            offset_token = self._consume(TokenType.NUMBER, "Expected number after OFFSET")
            offset = int(offset_token.value)
            
            # Optional LIMIT after OFFSET
            if self._match(TokenType.LIMIT):
                limit_token = self._consume(TokenType.NUMBER, "Expected number after LIMIT")
                limit = int(limit_token.value)
        
        # Build statement
        self._match(TokenType.SEMICOLON)
        
        return SelectStatement(
            columns=columns,
            tables=tables,
            where=where,
            joins=joins,
            group_by=group_by,
            having=having,
            order_by=order_by,
            distinct=distinct,
            limit=limit,
            offset=offset
        )
    
    def _parse_table_ref(self) -> TableRef:
        """Parse a table reference (name with optional alias)."""
        name_token = self._consume(TokenType.IDENTIFIER, "Expected table name")
        name = name_token.value
        
        alias = None
        if self._match(TokenType.AS):
            alias_token = self._consume(TokenType.IDENTIFIER, "Expected alias")
            alias = alias_token.value
        elif self._check(TokenType.IDENTIFIER) and not self._is_keyword(self.current):
            # Implicit alias
            alias_token = self._advance()
            alias = alias_token.value
        
        return TableRef(name=name, alias=alias)
    
    def _is_keyword(self, token: Token) -> bool:
        """Check if token is a keyword."""
        return token.type in (
            TokenType.SELECT, TokenType.FROM, TokenType.WHERE, TokenType.JOIN,
            TokenType.ON, TokenType.GROUP, TokenType.BY, TokenType.HAVING,
            TokenType.ORDER, TokenType.LIMIT, TokenType.OFFSET, TokenType.AND,
            TokenType.OR, TokenType.NOT, TokenType.AS
        )
    
    def _parse_join_type(self) -> Optional[str]:
        """Parse JOIN type keyword."""
        if self._match(TokenType.INNER):
            self._match(TokenType.JOIN)
            return "INNER"
        if self._match(TokenType.LEFT):
            self._match(TokenType.OUTER)
            self._match(TokenType.JOIN)
            return "LEFT"
        if self._match(TokenType.RIGHT):
            self._match(TokenType.OUTER)
            self._match(TokenType.JOIN)
            return "RIGHT"
        if self._match(TokenType.CROSS):
            self._match(TokenType.JOIN)
            return "CROSS"
        if self._match(TokenType.JOIN):
            return "INNER"
        return None
    
    # =========================================================================
    # Expressions
    # =========================================================================
    
    def _parse_expression(self) -> Expression:
        """Parse an expression (lowest precedence)."""
        return self._parse_or()
    
    def _parse_or(self) -> Expression:
        """Parse OR expression."""
        expr = self._parse_and()
        
        while self._match(TokenType.OR):
            right = self._parse_and()
            expr = BinaryOp(op="OR", left=expr, right=right)
        
        return expr
    
    def _parse_and(self) -> Expression:
        """Parse AND expression."""
        expr = self._parse_not()
        
        while self._match(TokenType.AND):
            right = self._parse_not()
            expr = BinaryOp(op="AND", left=expr, right=right)
        
        return expr
    
    def _parse_not(self) -> Expression:
        """Parse NOT expression."""
        if self._match(TokenType.NOT):
            expr = self._parse_not()
            return UnaryOp(op="NOT", expr=expr)
        return self._parse_comparison()
    
    def _parse_comparison(self) -> Expression:
        """Parse comparison expression."""
        expr = self._parse_additive()
        
        while True:
            if self._match(TokenType.EQUALS):
                right = self._parse_additive()
                expr = BinaryOp(op="=", left=expr, right=right)
            elif self._match(TokenType.NOT_EQUALS):
                right = self._parse_additive()
                expr = BinaryOp(op="!=", left=expr, right=right)
            elif self._match(TokenType.LESS):
                right = self._parse_additive()
                expr = BinaryOp(op="<", left=expr, right=right)
            elif self._match(TokenType.LESS_EQUAL):
                right = self._parse_additive()
                expr = BinaryOp(op="<=", left=expr, right=right)
            elif self._match(TokenType.GREATER):
                right = self._parse_additive()
                expr = BinaryOp(op=">", left=expr, right=right)
            elif self._match(TokenType.GREATER_EQUAL):
                right = self._parse_additive()
                expr = BinaryOp(op=">=", left=expr, right=right)
            elif self._match(TokenType.LIKE):
                right = self._parse_additive()
                expr = BinaryOp(op="LIKE", left=expr, right=right)
            else:
                break
        
        return expr
    
    def _parse_additive(self) -> Expression:
        """Parse addition/subtraction."""
        expr = self._parse_multiplicative()
        
        while True:
            if self._match(TokenType.PLUS):
                right = self._parse_multiplicative()
                expr = BinaryOp(op="+", left=expr, right=right)
            elif self._match(TokenType.MINUS):
                right = self._parse_multiplicative()
                expr = BinaryOp(op="-", left=expr, right=right)
            else:
                break
        
        return expr
    
    def _parse_multiplicative(self) -> Expression:
        """Parse multiplication/division."""
        expr = self._parse_unary()
        
        while True:
            if self._match(TokenType.STAR):
                right = self._parse_unary()
                expr = BinaryOp(op="*", left=expr, right=right)
            elif self._match(TokenType.SLASH):
                right = self._parse_unary()
                expr = BinaryOp(op="/", left=expr, right=right)
            elif self._match(TokenType.PERCENT):
                right = self._parse_unary()
                expr = BinaryOp(op="%", left=expr, right=right)
            else:
                break
        
        return expr
    
    def _parse_unary(self) -> Expression:
        """Parse unary expression."""
        if self._match(TokenType.MINUS):
            expr = self._parse_unary()
            return UnaryOp(op="-", expr=expr)
        if self._match(TokenType.PLUS):
            return self._parse_unary()
        return self._parse_primary()
    
    def _parse_primary(self) -> Expression:
        """Parse primary expression (literals, identifiers, function calls)."""
        # Parentheses
        if self._match(TokenType.LPAREN):
            expr = self._parse_expression()
            self._consume(TokenType.RPAREN, "Expected ')' after expression")
            return expr
        
        # String literal
        if self._check(TokenType.STRING):
            token = self._advance()
            return Literal(value=token.value, type="text")
        
        # Number literal
        if self._check(TokenType.NUMBER):
            token = self._advance()
            val = token.value
            if "." in val:
                return Literal(value=float(val), type="real")
            return Literal(value=int(val), type="int")
        
        # NULL
        if self._check(TokenType.NULL):
            self._advance()
            return Literal(value=None, type="null")
        
        # TRUE / FALSE
        if self._check(TokenType.TRUE):
            self._advance()
            return Literal(value=True, type="bool")
        if self._check(TokenType.FALSE):
            self._advance()
            return Literal(value=False, type="bool")
        
        # Identifier or function keyword (column reference or function call)
        if self._check(TokenType.IDENTIFIER) or self._check(
            TokenType.COUNT, TokenType.SUM, TokenType.AVG, 
            TokenType.MIN, TokenType.MAX
        ):
            token = self._advance()
            name = token.value
            
            # Check for function call
            if self._check(TokenType.LPAREN):
                self._advance()  # Consume (
                
                args: List[Expression] = []
                distinct = False
                
                if self._match(TokenType.DISTINCT):
                    distinct = True
                
                if not self._check(TokenType.RPAREN):
                    while True:
                        if self._check(TokenType.STAR):
                            self._advance()
                            args.append(ColumnRef("*"))
                        else:
                            args.append(self._parse_expression())
                        
                        if not self._match(TokenType.COMMA):
                            break
                
                self._consume(TokenType.RPAREN, "Expected ')' after function args")
                return FunctionCall(name=name, args=args, distinct=distinct)
            
            # Check for table.column (only for identifiers)
            if token.type == TokenType.IDENTIFIER and self._match(TokenType.DOT):
                col_token = self._consume(TokenType.IDENTIFIER, "Expected column name")
                return ColumnRef(name=col_token.value, table=name)
            
            return ColumnRef(name=name)
        
        # Wildcard *
        if self._check(TokenType.STAR):
            self._advance()
            return ColumnRef("*")
        
        raise ParseError(f"Unexpected token: {self.current}")
