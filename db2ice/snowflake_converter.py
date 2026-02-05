"""
Snowflake Standard to Iceberg Converter
Converts Snowflake FDN (Foundation/Standard) table DDL to Snowflake Managed Iceberg DDL
"""

import re
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
from enum import Enum


class IcebergCompatibility(Enum):
    """Iceberg compatibility status for Snowflake features"""
    COMPATIBLE = "compatible"
    NEEDS_CONVERSION = "needs_conversion"
    NOT_SUPPORTED = "not_supported"


@dataclass
class SnowflakeColumn:
    """Represents a Snowflake column definition"""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None
    identity: Optional[str] = None
    comment: Optional[str] = None
    collate: Optional[str] = None
    masking_policy: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class SnowflakeTable:
    """Represents a Snowflake table definition"""
    name: str
    schema: Optional[str] = None
    database: Optional[str] = None
    columns: List[SnowflakeColumn] = field(default_factory=list)
    cluster_by: Optional[List[str]] = None
    primary_key: Optional[List[str]] = None
    foreign_keys: List[dict] = field(default_factory=list)
    unique_keys: List[List[str]] = field(default_factory=list)
    comment: Optional[str] = None
    transient: bool = False
    temporary: bool = False
    dynamic: bool = False      # Dynamic tables - auto-refresh
    external: bool = False     # External tables - on stages
    hybrid: bool = False       # Hybrid tables - HTAP workload
    tags: List[str] = field(default_factory=list)
    data_retention_days: Optional[int] = None
    change_tracking: bool = False
    
    @property
    def full_name(self) -> str:
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.name)
        return ".".join(parts)


@dataclass
class ConversionIssue:
    """An issue found during conversion"""
    code: str
    severity: str  # 'critical', 'warning', 'info'
    message: str
    suggestion: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None


@dataclass
class SnowflakeToIcebergResult:
    """Result of Snowflake to Iceberg conversion"""
    iceberg_ddl: str
    tables_converted: int = 0
    ewi_count: int = 0
    success: bool = True
    error_message: Optional[str] = None
    issues: List[ConversionIssue] = field(default_factory=list)


class SnowflakeParser:
    """Parser for Snowflake DDL statements"""
    
    def parse(self, ddl: str) -> List[SnowflakeTable]:
        """Parse Snowflake DDL and return list of table definitions"""
        tables = []
        
        # Find all CREATE TABLE statements
        # Handle: CREATE [OR REPLACE] [TRANSIENT|TEMPORARY|DYNAMIC|EXTERNAL|HYBRID] TABLE
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:(TRANSIENT|TEMPORARY|DYNAMIC|EXTERNAL|HYBRID)\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)\s*\('
        
        for match in re.finditer(pattern, ddl, re.IGNORECASE):
            table_modifier = match.group(1)
            table_name = match.group(2).strip()
            
            # Find the matching closing parenthesis
            start_pos = match.end() - 1  # Position of opening paren
            column_defs, end_pos = self._extract_parenthesized_content(ddl, start_pos)
            
            if column_defs is None:
                continue
            
            # Get table options (everything after the closing paren until semicolon)
            rest = ddl[end_pos:].strip()
            semicolon_pos = rest.find(';')
            table_options = rest[:semicolon_pos] if semicolon_pos != -1 else rest
            
            table = self._parse_table(table_name, column_defs, table_options, table_modifier)
            if table:
                tables.append(table)
        
        return tables
    
    def _extract_parenthesized_content(self, text: str, start_pos: int) -> Tuple[Optional[str], int]:
        """Extract content between matching parentheses"""
        if text[start_pos] != '(':
            return None, start_pos
        
        depth = 0
        i = start_pos
        while i < len(text):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    # Return content without the outer parentheses
                    return text[start_pos + 1:i], i + 1
            i += 1
        
        return None, start_pos  # No matching close paren found
    
    def _parse_table(self, full_name: str, column_defs: str, options: str, modifier: Optional[str]) -> SnowflakeTable:
        """Parse a single table definition"""
        # Parse table name (handle database.schema.table format)
        name_parts = full_name.replace('"', '').split('.')
        
        # Determine table type from modifier
        modifier_upper = modifier.upper() if modifier else None
        
        table = SnowflakeTable(
            name=name_parts[-1],
            schema=name_parts[-2] if len(name_parts) >= 2 else None,
            database=name_parts[-3] if len(name_parts) >= 3 else None,
            transient=(modifier_upper == 'TRANSIENT'),
            temporary=(modifier_upper == 'TEMPORARY'),
            dynamic=(modifier_upper == 'DYNAMIC'),
            external=(modifier_upper == 'EXTERNAL'),
            hybrid=(modifier_upper == 'HYBRID')
        )
        
        # Parse columns and constraints
        self._parse_columns_and_constraints(table, column_defs)
        
        # Parse table options
        self._parse_table_options(table, options)
        
        return table
    
    def _parse_columns_and_constraints(self, table: SnowflakeTable, column_defs: str):
        """Parse column definitions and inline constraints"""
        # Split by comma, but handle nested parentheses
        parts = self._split_definitions(column_defs)
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            upper_part = part.upper()
            
            # Check if it's a constraint
            if upper_part.startswith('PRIMARY KEY'):
                # PRIMARY KEY (col1, col2)
                cols_match = re.search(r'\((.*?)\)', part)
                if cols_match:
                    table.primary_key = [c.strip().strip('"') for c in cols_match.group(1).split(',')]
            elif upper_part.startswith('FOREIGN KEY'):
                # FOREIGN KEY (col) REFERENCES table(col)
                fk_match = re.search(r'FOREIGN\s+KEY\s*\((.*?)\)\s*REFERENCES\s+([^\s(]+)\s*\((.*?)\)', part, re.IGNORECASE)
                if fk_match:
                    table.foreign_keys.append({
                        'columns': [c.strip().strip('"') for c in fk_match.group(1).split(',')],
                        'ref_table': fk_match.group(2).strip(),
                        'ref_columns': [c.strip().strip('"') for c in fk_match.group(3).split(',')]
                    })
            elif upper_part.startswith('UNIQUE'):
                # UNIQUE (col1, col2)
                cols_match = re.search(r'\((.*?)\)', part)
                if cols_match:
                    table.unique_keys.append([c.strip().strip('"') for c in cols_match.group(1).split(',')])
            elif upper_part.startswith('CONSTRAINT'):
                # Named constraint
                if 'PRIMARY KEY' in upper_part:
                    cols_match = re.search(r'PRIMARY\s+KEY\s*\((.*?)\)', part, re.IGNORECASE)
                    if cols_match:
                        table.primary_key = [c.strip().strip('"') for c in cols_match.group(1).split(',')]
                elif 'FOREIGN KEY' in upper_part:
                    fk_match = re.search(r'FOREIGN\s+KEY\s*\((.*?)\)\s*REFERENCES\s+([^\s(]+)\s*\((.*?)\)', part, re.IGNORECASE)
                    if fk_match:
                        table.foreign_keys.append({
                            'columns': [c.strip().strip('"') for c in fk_match.group(1).split(',')],
                            'ref_table': fk_match.group(2).strip(),
                            'ref_columns': [c.strip().strip('"') for c in fk_match.group(3).split(',')]
                        })
                elif 'UNIQUE' in upper_part:
                    cols_match = re.search(r'UNIQUE\s*\((.*?)\)', part, re.IGNORECASE)
                    if cols_match:
                        table.unique_keys.append([c.strip().strip('"') for c in cols_match.group(1).split(',')])
            else:
                # It's a column definition
                col = self._parse_column(part)
                if col:
                    table.columns.append(col)
    
    def _parse_column(self, col_def: str) -> Optional[SnowflakeColumn]:
        """Parse a single column definition"""
        # Pattern: column_name DATA_TYPE [options...]
        # Handle quoted identifiers
        if col_def.startswith('"'):
            name_match = re.match(r'"([^"]+)"\s+(.*)', col_def)
            if name_match:
                name = name_match.group(1)
                rest = name_match.group(2)
            else:
                return None
        else:
            parts = col_def.split(None, 1)
            if len(parts) < 2:
                return None
            name = parts[0]
            rest = parts[1]
        
        # Extract data type (handle types with parentheses like VARCHAR(100))
        type_match = re.match(r'(\w+(?:\s*\([^)]+\))?)', rest, re.IGNORECASE)
        if not type_match:
            return None
        
        data_type = type_match.group(1).upper()
        rest_of_def = rest[type_match.end():].strip()
        
        column = SnowflakeColumn(
            name=name.strip('"'),
            data_type=data_type
        )
        
        # Parse column options
        upper_rest = rest_of_def.upper()
        
        # NOT NULL
        if 'NOT NULL' in upper_rest:
            column.nullable = False
        
        # DEFAULT
        default_match = re.search(r'DEFAULT\s+([^\s,]+(?:\([^)]*\))?)', rest_of_def, re.IGNORECASE)
        if default_match:
            column.default = default_match.group(1)
        
        # IDENTITY
        if 'IDENTITY' in upper_rest or 'AUTOINCREMENT' in upper_rest:
            identity_match = re.search(r'(?:IDENTITY|AUTOINCREMENT)\s*(?:\(([^)]+)\))?', rest_of_def, re.IGNORECASE)
            column.identity = identity_match.group(1) if identity_match and identity_match.group(1) else "1,1"
        
        # COMMENT
        comment_match = re.search(r"COMMENT\s+'([^']*)'", rest_of_def, re.IGNORECASE)
        if comment_match:
            column.comment = comment_match.group(1)
        
        # COLLATE
        collate_match = re.search(r'COLLATE\s+([^\s,]+)', rest_of_def, re.IGNORECASE)
        if collate_match:
            column.collate = collate_match.group(1)
        
        # MASKING POLICY
        mask_match = re.search(r'WITH\s+MASKING\s+POLICY\s+([^\s,]+)', rest_of_def, re.IGNORECASE)
        if mask_match:
            column.masking_policy = mask_match.group(1)
        
        return column
    
    def _parse_table_options(self, table: SnowflakeTable, options: str):
        """Parse table-level options"""
        if not options:
            return
        
        upper_options = options.upper()
        
        # CLUSTER BY
        cluster_match = re.search(r'CLUSTER\s+BY\s*\((.*?)\)', options, re.IGNORECASE)
        if cluster_match:
            table.cluster_by = [c.strip().strip('"') for c in cluster_match.group(1).split(',')]
        
        # COMMENT
        comment_match = re.search(r"COMMENT\s*=\s*'([^']*)'", options, re.IGNORECASE)
        if comment_match:
            table.comment = comment_match.group(1)
        
        # DATA_RETENTION_TIME_IN_DAYS
        retention_match = re.search(r'DATA_RETENTION_TIME_IN_DAYS\s*=\s*(\d+)', options, re.IGNORECASE)
        if retention_match:
            table.data_retention_days = int(retention_match.group(1))
        
        # CHANGE_TRACKING
        if 'CHANGE_TRACKING' in upper_options:
            change_match = re.search(r'CHANGE_TRACKING\s*=\s*(TRUE|FALSE)', options, re.IGNORECASE)
            if change_match:
                table.change_tracking = change_match.group(1).upper() == 'TRUE'
    
    def _split_definitions(self, text: str) -> List[str]:
        """Split column/constraint definitions by comma, handling nested parentheses"""
        parts = []
        current = []
        depth = 0
        
        for char in text:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts


class SnowflakeToIcebergConverter:
    """
    Converts Snowflake Standard (FDN) tables to Snowflake Managed Iceberg tables.
    
    Key differences handled:
    - Add CATALOG = 'SNOWFLAKE', EXTERNAL_VOLUME, BASE_LOCATION
    - TRANSIENT/TEMPORARY tables flagged (not supported in Iceberg)
    - CLUSTER BY not directly supported (different optimization model)
    - IDENTITY/AUTOINCREMENT handled differently
    - Data retention, change tracking not applicable
    """
    
    EWI_TEMPLATE = "!!!RESOLVE EWI!!! /*** {code} - {message} ***/!!!"
    
    # Data types that need conversion for Iceberg
    # IMPORTANT: Iceberg does NOT support VARIANT, semi-structured OBJECT/ARRAY, GEOGRAPHY, GEOMETRY
    # Reference: https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types
    TYPE_CONVERSIONS = {
        # Semi-structured types - NOT SUPPORTED in Iceberg
        'VARIANT': ('VARCHAR', 'SSC-EWI-SF2ICE-0001', 'VARIANT not supported in Iceberg - converted to VARCHAR. Parse JSON at query time or use structured types'),
        'OBJECT': ('VARCHAR', 'SSC-EWI-SF2ICE-0002', 'Semi-structured OBJECT not supported in Iceberg - converted to VARCHAR. Use structured OBJECT with defined schema instead'),
        'ARRAY': ('VARCHAR', 'SSC-EWI-SF2ICE-0003', 'Semi-structured ARRAY not supported in Iceberg - converted to VARCHAR. Use structured ARRAY with defined element type instead'),
        
        # Spatial types - NOT SUPPORTED in Iceberg
        'GEOGRAPHY': ('VARCHAR', 'SSC-EWI-SF2ICE-0004', 'GEOGRAPHY not supported in Iceberg - converted to VARCHAR. Store as WKT/GeoJSON string'),
        'GEOMETRY': ('VARCHAR', 'SSC-EWI-SF2ICE-0005', 'GEOMETRY not supported in Iceberg - converted to VARCHAR. Store as WKT/GeoJSON string'),
    }
    
    # Timestamp types that need precision adjustment for Iceberg (must be precision 6)
    TIMESTAMP_TYPES = {
        'TIME': ('TIME(6)', 'SSC-EWI-SF2ICE-0006', 'TIME precision adjusted to 6 (microseconds) for Iceberg compatibility'),
        'TIMESTAMP': ('TIMESTAMP_NTZ(6)', 'SSC-EWI-SF2ICE-0007', 'TIMESTAMP precision adjusted to 6 (microseconds) for Iceberg compatibility'),
        'TIMESTAMP_NTZ': ('TIMESTAMP_NTZ(6)', 'SSC-EWI-SF2ICE-0007', 'TIMESTAMP_NTZ precision adjusted to 6 for Iceberg compatibility'),
        'TIMESTAMP_LTZ': ('TIMESTAMP_LTZ(6)', 'SSC-EWI-SF2ICE-0008', 'TIMESTAMP_LTZ precision adjusted to 6 for Iceberg compatibility'),
        'TIMESTAMP_TZ': ('TIMESTAMP_LTZ(6)', 'SSC-EWI-SF2ICE-0009', 'TIMESTAMP_TZ converted to TIMESTAMP_LTZ(6) for Iceberg compatibility'),
        'DATETIME': ('TIMESTAMP_NTZ(6)', 'SSC-EWI-SF2ICE-0007', 'DATETIME converted to TIMESTAMP_NTZ(6) for Iceberg compatibility'),
    }
    
    # Features not supported in Iceberg
    UNSUPPORTED_FEATURES = {
        'transient': ('SSC-EWI-SF2ICE-0010', 'TRANSIENT tables not supported in Iceberg - will be persistent'),
        'temporary': ('SSC-EWI-SF2ICE-0011', 'TEMPORARY tables not supported in Iceberg'),
        'cluster_by': ('SSC-EWI-SF2ICE-0012', 'CLUSTER BY not directly supported - Iceberg uses different optimization'),
        'data_retention': ('SSC-EWI-SF2ICE-0013', 'DATA_RETENTION_TIME_IN_DAYS not applicable to Iceberg tables'),
        'change_tracking': ('SSC-EWI-SF2ICE-0014', 'CHANGE_TRACKING not applicable to Iceberg tables'),
        'identity': ('SSC-EWI-SF2ICE-0015', 'IDENTITY/AUTOINCREMENT not supported in Iceberg tables'),
        'masking_policy': ('SSC-EWI-SF2ICE-0016', 'Masking policies need to be re-applied after conversion'),
        'collate': ('SSC-EWI-SF2ICE-0017', 'COLLATE clause not supported in Iceberg tables'),
    }
    
    def __init__(self,
                 external_volume: str = "<EXTERNAL_VOLUME>",
                 base_location_pattern: str = "{schema}/{table}",
                 include_comments: bool = True,
                 include_ewi: bool = True):
        self.external_volume = external_volume
        self.base_location_pattern = base_location_pattern
        self.include_comments = include_comments
        self.include_ewi = include_ewi
        self.parser = SnowflakeParser()
    
    def convert(self, ddl: str) -> SnowflakeToIcebergResult:
        """Convert Snowflake DDL to Iceberg DDL"""
        result = SnowflakeToIcebergResult(iceberg_ddl="")
        
        tables = self.parser.parse(ddl)
        
        if not tables:
            result.success = False
            result.error_message = "No valid CREATE TABLE statements found"
            return result
        
        converted_statements = []
        total_ewi = 0
        
        for table in tables:
            stmt, ewi_count, issues = self._convert_table(table)
            converted_statements.append(stmt)
            total_ewi += ewi_count
            result.issues.extend(issues)
        
        result.iceberg_ddl = "\n\n".join(converted_statements)
        result.tables_converted = len(tables)
        result.ewi_count = total_ewi
        
        return result
    
    def _convert_table(self, table: SnowflakeTable) -> Tuple[str, int, List[ConversionIssue]]:
        """Convert a single table to Iceberg format (or handle special table types)"""
        lines = []
        ewi_count = 0
        issues = []
        
        # === SPECIAL TABLE TYPES - Cannot convert to Iceberg ===
        
        # TEMPORARY tables: Keep as Snowflake Standard
        # Iceberg doesn't support temporary tables, converting would change behavior
        if table.temporary:
            return self._keep_as_standard_table(table, 'TEMPORARY')
        
        # TRANSIENT tables: Keep as Snowflake Standard  
        # Transient tables have no Fail-safe period - converting to Iceberg would add durability
        if table.transient:
            return self._keep_as_standard_table(table, 'TRANSIENT')
        
        # DYNAMIC tables: Skip entirely
        # Dynamic tables are auto-refreshing and fundamentally different from regular tables
        if table.dynamic:
            return self._skip_unsupported_table(table, 'DYNAMIC', 
                'Dynamic tables auto-refresh from a query and cannot be converted to Iceberg. '
                'Consider creating the underlying source tables as Iceberg instead.')
        
        # EXTERNAL tables: Skip entirely
        # External tables reference data in stages - already on external storage
        if table.external:
            return self._skip_unsupported_table(table, 'EXTERNAL',
                'External tables reference data in external stages. '
                'Consider using Iceberg tables with the same external volume instead.')
        
        # HYBRID tables: Skip entirely
        # Hybrid tables are for HTAP workloads with specific performance characteristics
        if table.hybrid:
            return self._skip_unsupported_table(table, 'HYBRID',
                'Hybrid tables are optimized for HTAP workloads. '
                'Iceberg tables have different performance characteristics for mixed workloads.')
        
        # === REGULAR TABLE - Convert to Iceberg ===
        
        # Header comment
        if self.include_comments:
            lines.append(f"-- Converted from Snowflake Standard: {table.full_name}")
        
        # CREATE ICEBERG TABLE
        table_name = self._format_name(table.full_name)
        lines.append(f"CREATE OR REPLACE ICEBERG TABLE {table_name} (")
        
        # Convert columns
        column_lines = []
        for i, col in enumerate(table.columns):
            col_line, col_ewi, col_issues = self._convert_column(col, table.full_name)
            ewi_count += col_ewi
            issues.extend(col_issues)
            
            # Add comma
            if i < len(table.columns) - 1 or table.primary_key:
                col_line += ","
            
            column_lines.append(col_line)
        
        # Add primary key if exists
        if table.primary_key:
            pk_cols = ", ".join(self._format_identifier(c) for c in table.primary_key)
            column_lines.append(f"    PRIMARY KEY ({pk_cols})")
        
        lines.extend(column_lines)
        lines.append(")")
        
        # Iceberg-specific clauses
        lines.append("CATALOG = 'SNOWFLAKE'")
        lines.append(f"EXTERNAL_VOLUME = '{self.external_volume}'")
        
        base_location = self._generate_base_location(table)
        lines.append(f"BASE_LOCATION = '{base_location}'")
        
        # Add comments for unsupported features
        if self.include_comments:
            comments = []
            
            if table.cluster_by:
                comments.append(f"-- Original CLUSTER BY: ({', '.join(table.cluster_by)})")
                comments.append("-- NOTE: Iceberg uses automatic optimization instead of explicit clustering")
                if self.include_ewi:
                    code, msg = self.UNSUPPORTED_FEATURES['cluster_by']
                    issues.append(ConversionIssue(code, 'info', msg, 
                                                  suggestion="Consider Iceberg table optimization strategies",
                                                  table_name=table.full_name))
            
            if table.data_retention_days:
                comments.append(f"-- Original DATA_RETENTION_TIME_IN_DAYS: {table.data_retention_days}")
            
            if table.change_tracking:
                comments.append("-- Original CHANGE_TRACKING: TRUE")
            
            # Foreign keys
            for fk in table.foreign_keys:
                cols = ", ".join(fk['columns'])
                ref_cols = ", ".join(fk['ref_columns'])
                comments.append(f"-- FOREIGN KEY ({cols}) REFERENCES {fk['ref_table']}({ref_cols})")
                comments.append("-- NOTE: Foreign keys are not enforced in Iceberg tables")
            
            # Unique constraints
            for uk in table.unique_keys:
                cols = ", ".join(uk)
                comments.append(f"-- UNIQUE ({cols})")
                comments.append("-- NOTE: UNIQUE constraints are not enforced in Iceberg tables")
            
            if table.comment:
                comments.append(f"-- Table comment: {table.comment}")
            
            if comments:
                lines.append("")
                lines.extend(comments)
        
        lines.append(";")
        
        return "\n".join(lines), ewi_count, issues
    
    def _keep_as_standard_table(self, table: SnowflakeTable, table_type: str) -> Tuple[str, int, List[ConversionIssue]]:
        """
        Keep TEMPORARY/TRANSIENT tables as Snowflake Standard (not Iceberg).
        
        Reasons:
        - TEMPORARY: Iceberg doesn't support session-scoped tables
        - TRANSIENT: Iceberg doesn't support no-Fail-safe tables
        
        Instead, we preserve the original DDL with an info message.
        """
        lines = []
        issues = []
        
        # Reasons for each table type
        reasons = {
            'TEMPORARY': (
                'Iceberg does not support temporary tables',
                'The table will remain session-scoped as originally intended',
                'SSC-EWI-SF2ICE-0020',
                'Table will remain session-scoped. Consider if temporary table is needed in target architecture.'
            ),
            'TRANSIENT': (
                'Iceberg tables always have durability (no transient option)',
                'The table will remain without Fail-safe as originally intended',
                'SSC-EWI-SF2ICE-0021',
                'Table will remain transient (no Fail-safe). Consider if transient behavior is needed or if Iceberg durability is acceptable.'
            )
        }
        
        reason_main, reason_detail, ewi_code, suggestion = reasons.get(table_type, reasons['TEMPORARY'])
        
        # Add explanatory comment
        if self.include_comments:
            lines.append(f"-- {table_type} table kept as Snowflake Standard (not converted to Iceberg)")
            lines.append(f"-- Reason: {reason_main}")
            lines.append(f"-- {reason_detail}")
        
        # Rebuild the original CREATE statement
        table_name = self._format_name(table.full_name)
        lines.append(f"CREATE OR REPLACE {table_type} TABLE {table_name} (")
        
        # Add columns (no type conversion needed - staying in Snowflake Standard)
        column_lines = []
        for i, col in enumerate(table.columns):
            col_line = self._format_standard_column(col)
            if i < len(table.columns) - 1 or table.primary_key:
                col_line += ","
            column_lines.append(col_line)
        
        # Add primary key if exists
        if table.primary_key:
            pk_cols = ", ".join(self._format_identifier(c) for c in table.primary_key)
            column_lines.append(f"    PRIMARY KEY ({pk_cols})")
        
        lines.extend(column_lines)
        lines.append(");")
        
        # Add info issue (not critical since we're preserving behavior)
        issues.append(ConversionIssue(
            code=ewi_code,
            severity='info',
            message=f'{table_type} table kept as Snowflake Standard - {reason_main}',
            suggestion=suggestion,
            table_name=table.full_name
        ))
        
        return "\n".join(lines), 0, issues  # No EWI markers in output (clean DDL)
    
    def _skip_unsupported_table(self, table: SnowflakeTable, table_type: str, reason: str) -> Tuple[str, int, List[ConversionIssue]]:
        """
        Skip tables that cannot be converted to Iceberg (DYNAMIC, EXTERNAL, HYBRID).
        
        These table types are fundamentally incompatible with Iceberg:
        - DYNAMIC: Auto-refresh from queries
        - EXTERNAL: Reference external stage data
        - HYBRID: HTAP-optimized storage
        """
        lines = []
        issues = []
        
        ewi_codes = {
            'DYNAMIC': 'SSC-EWI-SF2ICE-0022',
            'EXTERNAL': 'SSC-EWI-SF2ICE-0023',
            'HYBRID': 'SSC-EWI-SF2ICE-0024'
        }
        
        # Add comment explaining the skip
        if self.include_comments:
            lines.append(f"-- !!!! {table_type} TABLE SKIPPED - Cannot convert to Iceberg !!!!")
            lines.append(f"-- Table: {table.full_name}")
            lines.append(f"-- Reason: {reason}")
            lines.append(f"-- Action required: Review and handle this table manually")
        
        # Add critical issue
        issues.append(ConversionIssue(
            code=ewi_codes.get(table_type, 'SSC-EWI-SF2ICE-0025'),
            severity='critical',
            message=f'{table_type} table cannot be converted to Iceberg: {table.full_name}',
            suggestion=reason,
            table_name=table.full_name
        ))
        
        return "\n".join(lines), 1, issues  # Count as 1 EWI
    
    def _format_standard_column(self, col: SnowflakeColumn) -> str:
        """Format a column for Snowflake Standard table (preserve original syntax)"""
        parts = [f"    {self._format_identifier(col.name)}"]
        parts.append(col.data_type)
        
        if not col.nullable:
            parts.append("NOT NULL")
        
        if col.identity:
            parts.append(f"AUTOINCREMENT")
        
        if col.default:
            parts.append(f"DEFAULT {col.default}")
        
        return " ".join(parts)
    
    def _convert_column(self, col: SnowflakeColumn, table_name: str) -> Tuple[str, int, List[ConversionIssue]]:
        """Convert a column definition"""
        ewi_count = 0
        issues = []
        parts = []
        ewi_markers = []
        
        # Column name
        col_name = self._format_identifier(col.name)
        parts.append(f"    {col_name}")
        
        # Convert data type
        data_type = col.data_type
        base_type = re.match(r'(\w+)', data_type).group(1).upper() if data_type else 'VARCHAR'
        
        # Check for unsupported types (VARIANT, OBJECT, ARRAY, GEOGRAPHY, GEOMETRY)
        if base_type in self.TYPE_CONVERSIONS:
            new_type, ewi_code, ewi_msg = self.TYPE_CONVERSIONS[base_type]
            data_type = new_type
            if self.include_ewi:
                ewi_markers.append(self._format_ewi(ewi_code, ewi_msg))
                issues.append(ConversionIssue(ewi_code, 'critical', ewi_msg, 
                                              table_name=table_name, column_name=col.name))
                ewi_count += 1
        
        # Check for timestamp types that need precision adjustment to 6 (microseconds)
        elif base_type in self.TIMESTAMP_TYPES:
            # Extract current precision if specified
            precision_match = re.search(r'\((\d+)\)', data_type)
            current_precision = int(precision_match.group(1)) if precision_match else None
            
            # Only add EWI if precision is not 6
            new_type, ewi_code, ewi_msg = self.TIMESTAMP_TYPES[base_type]
            data_type = new_type
            
            if current_precision is not None and current_precision != 6:
                if self.include_ewi:
                    ewi_markers.append(self._format_ewi(ewi_code, ewi_msg))
                    issues.append(ConversionIssue(ewi_code, 'info', ewi_msg, 
                                                  table_name=table_name, column_name=col.name))
                    ewi_count += 1
        
        parts.append(data_type)
        
        # NOT NULL
        if not col.nullable:
            parts.append("NOT NULL")
        
        # Handle IDENTITY (not supported in Iceberg)
        if col.identity and self.include_ewi:
            code, msg = self.UNSUPPORTED_FEATURES['identity']
            ewi_markers.append(self._format_ewi(code, msg))
            issues.append(ConversionIssue(code, 'warning', msg,
                                          suggestion="Use application-generated IDs or sequences",
                                          table_name=table_name, column_name=col.name))
            ewi_count += 1
        
        # Handle masking policy
        if col.masking_policy and self.include_ewi:
            code, msg = self.UNSUPPORTED_FEATURES['masking_policy']
            ewi_markers.append(self._format_ewi(code, f"{msg}: {col.masking_policy}"))
            issues.append(ConversionIssue(code, 'warning', msg,
                                          suggestion=f"Re-apply masking policy {col.masking_policy} after conversion",
                                          table_name=table_name, column_name=col.name))
            ewi_count += 1
        
        # Handle collate
        if col.collate and self.include_ewi:
            code, msg = self.UNSUPPORTED_FEATURES['collate']
            ewi_markers.append(self._format_ewi(code, f"{msg}: {col.collate}"))
            issues.append(ConversionIssue(code, 'info', msg,
                                          table_name=table_name, column_name=col.name))
            ewi_count += 1
        
        # Build line
        line = " ".join(parts)
        
        # Add EWI markers
        if ewi_markers:
            line += "\n" + "\n".join(f"        {ewi}" for ewi in ewi_markers)
        
        return line, ewi_count, issues
    
    def _format_ewi(self, code: str, message: str) -> str:
        """Format an EWI marker"""
        return self.EWI_TEMPLATE.format(code=code, message=message)
    
    def _format_identifier(self, identifier: str) -> str:
        """Format an identifier"""
        reserved = {'ORDER', 'GROUP', 'SELECT', 'FROM', 'WHERE', 'TABLE', 'INDEX',
                    'CREATE', 'DROP', 'ALTER', 'INSERT', 'UPDATE', 'DELETE', 'VALUES',
                    'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'DATE', 'TIME', 'TIMESTAMP'}
        
        upper = identifier.upper()
        if upper in reserved or not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
            return f'"{identifier}"'
        return identifier.upper()
    
    def _format_name(self, name: str) -> str:
        """Format a table name"""
        return name.upper()
    
    def _generate_base_location(self, table: SnowflakeTable) -> str:
        """Generate base location from pattern"""
        location = self.base_location_pattern
        schema = table.schema if table.schema else "default"
        location = location.replace("{schema}", schema.lower())
        location = location.replace("{table}", table.name.lower())
        return location
