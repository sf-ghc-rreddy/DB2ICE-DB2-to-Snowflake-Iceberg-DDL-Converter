"""
DB2 DDL Parser - Tokenizes and parses DB2 CREATE TABLE statements
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class DB2DataType(Enum):
    """DB2 data types"""
    # Numeric
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    INT = "INT"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    DEC = "DEC"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECFLOAT = "DECFLOAT"
    
    # Character
    CHAR = "CHAR"
    CHARACTER = "CHARACTER"
    VARCHAR = "VARCHAR"
    LONG_VARCHAR = "LONG VARCHAR"
    CLOB = "CLOB"
    
    # Graphic (DBCS)
    GRAPHIC = "GRAPHIC"
    VARGRAPHIC = "VARGRAPHIC"
    LONG_VARGRAPHIC = "LONG VARGRAPHIC"
    DBCLOB = "DBCLOB"
    
    # Binary
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BLOB = "BLOB"
    
    # Date/Time
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    
    # Special
    XML = "XML"
    ROWID = "ROWID"
    
    # Boolean (DB2 11.1+)
    BOOLEAN = "BOOLEAN"


@dataclass
class Column:
    """Represents a DB2 column definition"""
    name: str
    data_type: str
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    default: Optional[str] = None
    generated: Optional[str] = None  # ALWAYS, BY DEFAULT
    ccsid: Optional[str] = None  # ASCII, UNICODE, etc.
    for_bit_data: bool = False
    fieldproc: Optional[str] = None
    raw_definition: str = ""


@dataclass
class Constraint:
    """Represents a table constraint"""
    type: str  # PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
    name: Optional[str] = None
    columns: list = field(default_factory=list)
    reference_table: Optional[str] = None
    reference_columns: list = field(default_factory=list)
    check_condition: Optional[str] = None


@dataclass
class PartitionSpec:
    """Represents partition specification"""
    type: str  # RANGE, HASH
    columns: list = field(default_factory=list)
    partitions: list = field(default_factory=list)
    raw_definition: str = ""


@dataclass
class TableDefinition:
    """Represents a complete DB2 table definition"""
    schema: Optional[str] = None
    name: str = ""
    columns: list = field(default_factory=list)
    constraints: list = field(default_factory=list)
    partition: Optional[PartitionSpec] = None
    tablespace: Optional[str] = None
    editproc: Optional[str] = None
    validproc: Optional[str] = None
    audit: Optional[str] = None
    data_capture: Optional[str] = None
    ccsid: Optional[str] = None
    volatile: bool = False              # VOLATILE table (session-scoped)
    global_temporary: bool = False      # CREATED GLOBAL TEMPORARY table
    raw_ddl: str = ""
    
    @property
    def full_name(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name


class DB2Parser:
    """Parser for DB2 CREATE TABLE statements"""
    
    # Regex patterns
    # Extended pattern to handle VOLATILE and GLOBAL TEMPORARY tables
    # DB2 supports: CREATE TABLE, CREATE VOLATILE TABLE, CREATE GLOBAL TEMPORARY TABLE
    # Also: DECLARE GLOBAL TEMPORARY TABLE (session-scoped)
    CREATE_TABLE_PATTERN = re.compile(
        r'CREATE\s+(?:(VOLATILE)\s+)?(?:(GLOBAL\s+TEMPORARY)\s+)?TABLE\s+(?:(["\w]+)\.)?(["\w]+)\s*\(',
        re.IGNORECASE
    )
    
    # Alternate pattern for DECLARE GLOBAL TEMPORARY TABLE
    DECLARE_TEMP_PATTERN = re.compile(
        r'DECLARE\s+GLOBAL\s+TEMPORARY\s+TABLE\s+(?:(["\w]+)\.)?(["\w]+)\s*\(',
        re.IGNORECASE
    )
    
    DATA_TYPE_PATTERN = re.compile(
        r'(SMALLINT|INTEGER|INT|BIGINT|DECIMAL|DEC|NUMERIC|REAL|FLOAT|DOUBLE|DECFLOAT|'
        r'CHARACTER\s+VARYING|CHAR\s+VARYING|VARCHAR|LONG\s+VARCHAR|CHARACTER|CHAR|CLOB|'
        r'GRAPHIC|VARGRAPHIC|LONG\s+VARGRAPHIC|DBCLOB|'
        r'BINARY\s+VARYING|VARBINARY|BINARY|BLOB|'
        r'DATE|TIMESTAMP|TIME|XML|ROWID|BOOLEAN)'
        r'(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?',
        re.IGNORECASE
    )
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def parse(self, ddl: str) -> list[TableDefinition]:
        """Parse DDL string and return list of table definitions"""
        tables = []
        self.errors = []
        self.warnings = []
        
        # Split into individual statements
        statements = self._split_statements(ddl)
        
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            
            # Remove leading comments to check statement type
            stmt_without_comments = self._strip_leading_comments(stmt)
                
            # Process CREATE TABLE statements (including VOLATILE and GLOBAL TEMPORARY)
            if re.match(r'^\s*CREATE\s+(?:VOLATILE\s+)?(?:GLOBAL\s+TEMPORARY\s+)?TABLE', stmt_without_comments, re.IGNORECASE):
                try:
                    table = self._parse_create_table(stmt_without_comments)
                    if table:
                        tables.append(table)
                except Exception as e:
                    self.errors.append(f"Failed to parse statement: {str(e)}")
            
            # Process DECLARE GLOBAL TEMPORARY TABLE statements
            elif re.match(r'^\s*DECLARE\s+GLOBAL\s+TEMPORARY\s+TABLE', stmt_without_comments, re.IGNORECASE):
                try:
                    table = self._parse_declare_temp_table(stmt_without_comments)
                    if table:
                        tables.append(table)
                except Exception as e:
                    self.errors.append(f"Failed to parse DECLARE statement: {str(e)}")
        
        return tables
    
    def _strip_leading_comments(self, stmt: str) -> str:
        """Remove leading SQL comments from a statement"""
        lines = stmt.split('\n')
        result_lines = []
        found_code = False
        
        for line in lines:
            stripped = line.strip()
            # Skip comment-only lines at the beginning
            if not found_code and (stripped.startswith('--') or not stripped):
                continue
            found_code = True
            result_lines.append(line)
        
        return '\n'.join(result_lines)
    
    def _split_statements(self, ddl: str) -> list[str]:
        """Split DDL into individual statements"""
        # Handle both ; and @ as statement terminators (DB2 uses @ in scripts)
        statements = []
        current = []
        in_string = False
        paren_depth = 0
        
        i = 0
        while i < len(ddl):
            char = ddl[i]
            
            # Track string literals
            if char == "'" and (i == 0 or ddl[i-1] != '\\'):
                in_string = not in_string
            
            # Track parentheses (not in strings)
            if not in_string:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
            
            # Statement terminator
            if char in (';', '@') and not in_string and paren_depth == 0:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)
            
            i += 1
        
        # Handle last statement without terminator
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)
        
        return statements
    
    def _parse_create_table(self, stmt: str) -> Optional[TableDefinition]:
        """Parse a CREATE TABLE statement (including VOLATILE and GLOBAL TEMPORARY)"""
        table = TableDefinition(raw_ddl=stmt)
        
        # Extract schema, table name, and modifiers
        match = self.CREATE_TABLE_PATTERN.search(stmt)
        if not match:
            self.errors.append("Could not parse table name")
            return None
        
        # Group 1: VOLATILE modifier
        # Group 2: GLOBAL TEMPORARY modifier
        # Group 3: Schema name
        # Group 4: Table name
        is_volatile = match.group(1) is not None
        is_global_temp = match.group(2) is not None
        
        table.volatile = is_volatile
        table.global_temporary = is_global_temp
        table.schema = self._clean_identifier(match.group(3)) if match.group(3) else None
        table.name = self._clean_identifier(match.group(4))
        
        # Extract column definitions (content between first ( and matching ))
        col_start = stmt.find('(', match.end() - 1)
        if col_start == -1:
            self.errors.append("Could not find column definitions")
            return None
        
        # Find the matching closing parenthesis
        col_end = self._find_matching_paren(stmt, col_start)
        if col_end == -1:
            self.errors.append("Could not find end of column definitions")
            return None
        
        columns_str = stmt[col_start + 1:col_end]
        
        # Parse columns and constraints
        self._parse_columns_and_constraints(columns_str, table)
        
        # Parse table options (after the column definitions)
        options_str = stmt[col_end + 1:]
        self._parse_table_options(options_str, table)
        
        return table
    
    def _parse_declare_temp_table(self, stmt: str) -> Optional[TableDefinition]:
        """Parse a DECLARE GLOBAL TEMPORARY TABLE statement"""
        table = TableDefinition(raw_ddl=stmt)
        table.global_temporary = True  # DECLARE is always global temporary
        
        # Extract schema and table name
        match = self.DECLARE_TEMP_PATTERN.search(stmt)
        if not match:
            self.errors.append("Could not parse DECLARE GLOBAL TEMPORARY TABLE")
            return None
        
        table.schema = self._clean_identifier(match.group(1)) if match.group(1) else None
        table.name = self._clean_identifier(match.group(2))
        
        # Extract column definitions
        col_start = stmt.find('(', match.end() - 1)
        if col_start == -1:
            self.errors.append("Could not find column definitions")
            return None
        
        col_end = self._find_matching_paren(stmt, col_start)
        if col_end == -1:
            self.errors.append("Could not find end of column definitions")
            return None
        
        columns_str = stmt[col_start + 1:col_end]
        self._parse_columns_and_constraints(columns_str, table)
        
        # Parse table options
        options_str = stmt[col_end + 1:]
        self._parse_table_options(options_str, table)
        
        return table
    
    def _find_matching_paren(self, s: str, start: int) -> int:
        """Find the matching closing parenthesis"""
        depth = 0
        in_string = False
        
        for i in range(start, len(s)):
            char = s[i]
            
            if char == "'" and (i == 0 or s[i-1] != '\\'):
                in_string = not in_string
            
            if not in_string:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                    if depth == 0:
                        return i
        
        return -1
    
    def _parse_columns_and_constraints(self, columns_str: str, table: TableDefinition):
        """Parse column definitions and inline constraints"""
        # Split by comma, respecting parentheses and strings
        parts = self._split_column_defs(columns_str)
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Check if it's a constraint
            if self._is_constraint(part):
                constraint = self._parse_constraint(part)
                if constraint:
                    table.constraints.append(constraint)
            else:
                column = self._parse_column(part)
                if column:
                    table.columns.append(column)
    
    def _split_column_defs(self, s: str) -> list[str]:
        """Split column definitions by comma, respecting parentheses"""
        parts = []
        current = []
        paren_depth = 0
        in_string = False
        
        for char in s:
            if char == "'" and (not current or current[-1] != '\\'):
                in_string = not in_string
            
            if not in_string:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    parts.append(''.join(current))
                    current = []
                    continue
            
            current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _is_constraint(self, part: str) -> bool:
        """Check if the part is a constraint definition"""
        constraint_keywords = [
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'CONSTRAINT'
        ]
        upper = part.upper().strip()
        return any(upper.startswith(kw) or f' {kw}' in upper for kw in constraint_keywords)
    
    def _parse_column(self, col_def: str) -> Optional[Column]:
        """Parse a single column definition"""
        col_def = col_def.strip()
        if not col_def:
            return None
        
        column = Column(name="", data_type="", raw_definition=col_def)
        
        # Extract column name (first identifier)
        name_match = re.match(r'^(["\w]+)', col_def)
        if not name_match:
            self.warnings.append(f"Could not parse column name: {col_def[:50]}")
            return None
        
        column.name = self._clean_identifier(name_match.group(1))
        remaining = col_def[name_match.end():].strip()
        
        # Extract data type
        type_match = self.DATA_TYPE_PATTERN.match(remaining)
        if type_match:
            column.data_type = type_match.group(1).upper()
            # Normalize multi-word types
            column.data_type = re.sub(r'\s+', ' ', column.data_type)
            
            if type_match.group(2):
                column.length = int(type_match.group(2))
                column.precision = column.length
            if type_match.group(3):
                column.scale = int(type_match.group(3))
            
            remaining = remaining[type_match.end():].strip()
        else:
            self.warnings.append(f"Could not parse data type for column {column.name}")
            return None
        
        # Parse column attributes
        upper_remaining = remaining.upper()
        
        # NOT NULL
        if 'NOT NULL' in upper_remaining:
            column.nullable = False
        
        # DEFAULT
        default_match = re.search(r'DEFAULT\s+(\S+|\'[^\']*\')', remaining, re.IGNORECASE)
        if default_match:
            column.default = default_match.group(1)
        
        # GENERATED
        if 'GENERATED ALWAYS' in upper_remaining:
            column.generated = 'ALWAYS'
        elif 'GENERATED BY DEFAULT' in upper_remaining:
            column.generated = 'BY DEFAULT'
        
        # FOR BIT DATA
        if 'FOR BIT DATA' in upper_remaining:
            column.for_bit_data = True
        
        # CCSID
        ccsid_match = re.search(r'CCSID\s+(\w+)', remaining, re.IGNORECASE)
        if ccsid_match:
            column.ccsid = ccsid_match.group(1)
        
        # FIELDPROC
        fieldproc_match = re.search(r'FIELDPROC\s+(\S+)', remaining, re.IGNORECASE)
        if fieldproc_match:
            column.fieldproc = fieldproc_match.group(1)
        
        return column
    
    def _parse_constraint(self, constraint_def: str) -> Optional[Constraint]:
        """Parse a constraint definition"""
        constraint = Constraint(type="")
        upper = constraint_def.upper()
        
        # Extract constraint name
        name_match = re.match(r'CONSTRAINT\s+(["\w]+)', constraint_def, re.IGNORECASE)
        if name_match:
            constraint.name = self._clean_identifier(name_match.group(1))
        
        # Determine constraint type and parse
        if 'PRIMARY KEY' in upper:
            constraint.type = 'PRIMARY KEY'
            cols_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', constraint_def, re.IGNORECASE)
            if cols_match:
                constraint.columns = [self._clean_identifier(c.strip()) 
                                     for c in cols_match.group(1).split(',')]
        
        elif 'FOREIGN KEY' in upper:
            constraint.type = 'FOREIGN KEY'
            fk_match = re.search(
                r'FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(["\w.]+)\s*\(([^)]+)\)',
                constraint_def, re.IGNORECASE
            )
            if fk_match:
                constraint.columns = [self._clean_identifier(c.strip()) 
                                     for c in fk_match.group(1).split(',')]
                constraint.reference_table = fk_match.group(2)
                constraint.reference_columns = [self._clean_identifier(c.strip()) 
                                               for c in fk_match.group(3).split(',')]
        
        elif 'UNIQUE' in upper:
            constraint.type = 'UNIQUE'
            cols_match = re.search(r'UNIQUE\s*\(([^)]+)\)', constraint_def, re.IGNORECASE)
            if cols_match:
                constraint.columns = [self._clean_identifier(c.strip()) 
                                     for c in cols_match.group(1).split(',')]
        
        elif 'CHECK' in upper:
            constraint.type = 'CHECK'
            check_match = re.search(r'CHECK\s*\((.+)\)', constraint_def, re.IGNORECASE | re.DOTALL)
            if check_match:
                constraint.check_condition = check_match.group(1).strip()
        
        return constraint if constraint.type else None
    
    def _parse_table_options(self, options_str: str, table: TableDefinition):
        """Parse table-level options"""
        upper = options_str.upper()
        
        # IN tablespace
        ts_match = re.search(r'IN\s+(["\w]+)', options_str, re.IGNORECASE)
        if ts_match:
            table.tablespace = self._clean_identifier(ts_match.group(1))
        
        # EDITPROC
        if 'EDITPROC' in upper:
            editproc_match = re.search(r'EDITPROC\s+(["\w.]+)', options_str, re.IGNORECASE)
            if editproc_match:
                table.editproc = editproc_match.group(1)
        
        # VALIDPROC
        if 'VALIDPROC' in upper:
            validproc_match = re.search(r'VALIDPROC\s+(["\w.]+)', options_str, re.IGNORECASE)
            if validproc_match:
                table.validproc = validproc_match.group(1)
        
        # AUDIT
        if 'AUDIT' in upper:
            audit_match = re.search(r'AUDIT\s+(NONE|CHANGES|ALL)', options_str, re.IGNORECASE)
            if audit_match:
                table.audit = audit_match.group(1).upper()
        
        # DATA CAPTURE
        if 'DATA CAPTURE' in upper:
            dc_match = re.search(r'DATA\s+CAPTURE\s+(NONE|CHANGES)', options_str, re.IGNORECASE)
            if dc_match:
                table.data_capture = dc_match.group(1).upper()
        
        # CCSID
        ccsid_match = re.search(r'CCSID\s+(ASCII|UNICODE|EBCDIC)', options_str, re.IGNORECASE)
        if ccsid_match:
            table.ccsid = ccsid_match.group(1).upper()
        
        # PARTITION BY
        if 'PARTITION BY' in upper:
            self._parse_partition(options_str, table)
    
    def _parse_partition(self, options_str: str, table: TableDefinition):
        """Parse partition specification"""
        partition_match = re.search(
            r'PARTITION\s+BY\s+(RANGE|HASH)\s*\(([^)]+)\)',
            options_str, re.IGNORECASE
        )
        
        if partition_match:
            table.partition = PartitionSpec(
                type=partition_match.group(1).upper(),
                columns=[self._clean_identifier(c.strip()) 
                        for c in partition_match.group(2).split(',')],
                raw_definition=partition_match.group(0)
            )
    
    def _clean_identifier(self, identifier: str) -> str:
        """Remove quotes from identifier if present"""
        if identifier is None:
            return ""
        return identifier.strip('"').strip("'").strip('`')
