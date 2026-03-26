"""
SQL Tokenizer (Lexer)

Breaks SQL text into a stream of tokens (keywords, identifiers, literals, operators).
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List


class TokenType(Enum):
    """Token types for SQL tokenization."""
    # Keywords
    CREATE = auto()
    TABLE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    NULL = auto()
    TRUE = auto()
    FALSE = auto()
    INT = auto()
    TEXT = auto()
    REAL = auto()
    BOOL = auto()
    PRIMARY = auto()
    KEY = auto()
    BEGIN = auto()
    COMMIT = auto()
    ROLLBACK = auto()
    DELETE = auto()
    UPDATE = auto()
    SET = auto()
    JOIN = auto()
    ON = auto()
    GROUP = auto()
    BY = auto()
    HAVING = auto()
    ORDER = auto()
    ASC = auto()
    DESC = auto()
    AS = auto()
    IS = auto()
    IN = auto()
    LIKE = auto()
    LEFT = auto()
    RIGHT = auto()
    INNER = auto()
    OUTER = auto()
    CROSS = auto()
    DISTINCT = auto()
    COUNT = auto()
    SUM = auto()
    AVG = auto()
    MIN = auto()
    MAX = auto()
    DEFAULT = auto()
    LIMIT = auto()
    OFFSET = auto()
    
    # Identifiers and literals
    IDENTIFIER = auto()
    STRING = auto()
    NUMBER = auto()
    
    # Operators
    EQUALS = auto()
    NOT_EQUALS = auto()
    LESS = auto()
    LESS_EQUAL = auto()
    GREATER = auto()
    GREATER_EQUAL = auto()
    PLUS = auto()
    MINUS = auto()
    STAR = auto()
    SLASH = auto()
    PERCENT = auto()
    
    # Punctuation
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    SEMICOLON = auto()
    DOT = auto()
    
    # Special
    EOF = auto()
    UNKNOWN = auto()


@dataclass
class Token:
    """Represents a single token from the lexer."""
    type: TokenType
    value: str
    line: int
    column: int
    
    def __repr__(self):
        return f"Token({self.type.name}, {self.value!r}, line={self.line}, col={self.column})"


# SQL Keywords mapping (lowercase -> TokenType)
KEYWORDS = {
    "create": TokenType.CREATE,
    "table": TokenType.TABLE,
    "insert": TokenType.INSERT,
    "into": TokenType.INTO,
    "values": TokenType.VALUES,
    "select": TokenType.SELECT,
    "from": TokenType.FROM,
    "where": TokenType.WHERE,
    "and": TokenType.AND,
    "or": TokenType.OR,
    "not": TokenType.NOT,
    "null": TokenType.NULL,
    "true": TokenType.TRUE,
    "false": TokenType.FALSE,
    "int": TokenType.INT,
    "text": TokenType.TEXT,
    "real": TokenType.REAL,
    "bool": TokenType.BOOL,
    "primary": TokenType.PRIMARY,
    "key": TokenType.KEY,
    "begin": TokenType.BEGIN,
    "commit": TokenType.COMMIT,
    "rollback": TokenType.ROLLBACK,
    "delete": TokenType.DELETE,
    "update": TokenType.UPDATE,
    "set": TokenType.SET,
    "join": TokenType.JOIN,
    "on": TokenType.ON,
    "group": TokenType.GROUP,
    "by": TokenType.BY,
    "having": TokenType.HAVING,
    "order": TokenType.ORDER,
    "asc": TokenType.ASC,
    "desc": TokenType.DESC,
    "as": TokenType.AS,
    "is": TokenType.IS,
    "in": TokenType.IN,
    "like": TokenType.LIKE,
    "left": TokenType.LEFT,
    "right": TokenType.RIGHT,
    "inner": TokenType.INNER,
    "outer": TokenType.OUTER,
    "cross": TokenType.CROSS,
    "distinct": TokenType.DISTINCT,
    "count": TokenType.COUNT,
    "sum": TokenType.SUM,
    "avg": TokenType.AVG,
    "min": TokenType.MIN,
    "max": TokenType.MAX,
    "default": TokenType.DEFAULT,
    "limit": TokenType.LIMIT,
    "offset": TokenType.OFFSET,
}


class Tokenizer:
    """
    SQL Tokenizer that converts SQL text into a stream of tokens.
    
    Usage:
        tokenizer = Tokenizer("SELECT * FROM users")
        tokens = tokenizer.tokenize()
    """
    
    def __init__(self, sql: str):
        self.sql = sql
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []
    
    def _peek(self, offset: int = 0) -> str:
        """Peek at character at current position + offset."""
        idx = self.pos + offset
        if idx < len(self.sql):
            return self.sql[idx]
        return ""
    
    def _advance(self, n: int = 1) -> str:
        """Advance position by n characters and return the last one."""
        char = ""
        for _ in range(n):
            if self.pos < len(self.sql):
                char = self.sql[self.pos]
                if char == "\n":
                    self.line += 1
                    self.column = 1
                else:
                    self.column += 1
                self.pos += 1
        return char
    
    def _skip_whitespace(self):
        """Skip whitespace characters."""
        while self._peek() and self._peek() in " \t\r\n":
            self._advance()
    
    def _read_string(self) -> Token:
        """Read a string literal (single-quoted)."""
        start_line = self.line
        start_col = self.column
        
        self._advance()  # Skip opening quote
        value = ""
        
        while self._peek() and self._peek() != "'":
            if self._peek() == "\\" and self._peek(1) == "'":
                self._advance()  # Skip backslash
                value += self._advance()  # Add quote
            else:
                value += self._advance()
        
        if self._peek() == "'":
            self._advance()  # Skip closing quote
        else:
            # Unterminated string - but we'll return what we have
            pass
        
        return Token(TokenType.STRING, value, start_line, start_col)
    
    def _read_number(self) -> Token:
        """Read a numeric literal (integer or float)."""
        start_line = self.line
        start_col = self.column
        
        value = ""
        
        # Read digits before decimal
        while self._peek().isdigit():
            value += self._advance()
        
        # Check for decimal point
        if self._peek() == "." and self._peek(1).isdigit():
            value += self._advance()  # Add the dot
            while self._peek().isdigit():
                value += self._advance()
        
        return Token(TokenType.NUMBER, value, start_line, start_col)
    
    def _read_identifier_or_keyword(self) -> Token:
        """Read an identifier or keyword."""
        start_line = self.line
        start_col = self.column
        
        value = ""
        
        # First character: letter or underscore
        if self._peek().isalpha() or self._peek() == "_":
            value += self._advance()
        
        # Subsequent: letters, digits, underscores
        while self._peek().isalnum() or self._peek() == "_":
            value += self._advance()
        
        # Check if it's a keyword (case-insensitive)
        lower = value.lower()
        if lower in KEYWORDS:
            return Token(KEYWORDS[lower], value, start_line, start_col)
        
        return Token(TokenType.IDENTIFIER, value, start_line, start_col)
    
    def _read_operator_or_punct(self) -> Optional[Token]:
        """Read an operator or punctuation token."""
        start_line = self.line
        start_col = self.column
        
        char = self._peek()
        
        # Two-character operators
        if char == "=":
            self._advance()
            return Token(TokenType.EQUALS, "=", start_line, start_col)
        
        if char == "!":
            if self._peek(1) == "=":
                self._advance(2)
                return Token(TokenType.NOT_EQUALS, "!=", start_line, start_col)
            self._advance()
            return Token(TokenType.UNKNOWN, "!", start_line, start_col)
        
        if char == "<":
            if self._peek(1) == "=":
                self._advance(2)
                return Token(TokenType.LESS_EQUAL, "<=", start_line, start_col)
            if self._peek(1) == ">":
                self._advance(2)
                return Token(TokenType.NOT_EQUALS, "<>", start_line, start_col)
            self._advance()
            return Token(TokenType.LESS, "<", start_line, start_col)
        
        if char == ">":
            if self._peek(1) == "=":
                self._advance(2)
                return Token(TokenType.GREATER_EQUAL, ">=", start_line, start_col)
            self._advance()
            return Token(TokenType.GREATER, ">", start_line, start_col)
        
        if char == "+":
            self._advance()
            return Token(TokenType.PLUS, "+", start_line, start_col)
        
        if char == "-":
            self._advance()
            return Token(TokenType.MINUS, "-", start_line, start_col)
        
        if char == "*":
            self._advance()
            return Token(TokenType.STAR, "*", start_line, start_col)
        
        if char == "/":
            self._advance()
            return Token(TokenType.SLASH, "/", start_line, start_col)
        
        if char == "%":
            self._advance()
            return Token(TokenType.PERCENT, "%", start_line, start_col)
        
        # Punctuation
        if char == "(":
            self._advance()
            return Token(TokenType.LPAREN, "(", start_line, start_col)
        
        if char == ")":
            self._advance()
            return Token(TokenType.RPAREN, ")", start_line, start_col)
        
        if char == ",":
            self._advance()
            return Token(TokenType.COMMA, ",", start_line, start_col)
        
        if char == ";":
            self._advance()
            return Token(TokenType.SEMICOLON, ";", start_line, start_col)
        
        if char == ".":
            self._advance()
            return Token(TokenType.DOT, ".", start_line, start_col)
        
        return None
    
    def tokenize(self) -> List[Token]:
        """Tokenize the entire SQL string and return list of tokens."""
        self.tokens = []
        
        while self.pos < len(self.sql):
            self._skip_whitespace()
            
            if self.pos >= len(self.sql):
                break
            
            char = self._peek()
            
            # String literal
            if char == "'":
                self.tokens.append(self._read_string())
                continue
            
            # Number literal
            if char.isdigit():
                self.tokens.append(self._read_number())
                continue
            
            # Identifier or keyword (starts with letter or underscore)
            if char.isalpha() or char == "_":
                self.tokens.append(self._read_identifier_or_keyword())
                continue
            
            # Operators and punctuation
            token = self._read_operator_or_punct()
            if token:
                self.tokens.append(token)
                continue
            
            # Unknown character - skip it
            self._advance()
        
        # Add EOF token
        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        
        return self.tokens
    
    def __iter__(self):
        """Iterate over tokens (lazily tokenizes on first iteration)."""
        if not self.tokens:
            self.tokenize()
        return iter(self.tokens)
