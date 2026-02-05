"""
DDL Converter - Converts DB2 DDL to Snowflake Iceberg DDL with EWI annotations
"""

from dataclasses import dataclass, field
from typing import Optional
import re

from db2ice.parser import DB2Parser, TableDefinition, Column, Constraint
from db2ice.mapper import DataTypeMapper, ConversionStatus
from db2ice.assessor import Assessor, AssessmentReport


@dataclass
class ConversionResult:
    """Result of DDL conversion"""
    iceberg_ddl: str
    assessment: AssessmentReport
    ewi_count: int = 0
    tables_converted: int = 0
    success: bool = True
    error_message: Optional[str] = None


class DB2IceConverter:
    """
    Converts DB2 DDL to Snowflake Managed Iceberg Table DDL.
    
    Features:
    - Full type mapping with EWI annotations
    - Constraint documentation (not enforcement in Iceberg)
    - Assessment report generation
    - Configurable external volume and base location
    """
    
    # EWI marker format (SnowConvert style)
    EWI_TEMPLATE = "!!!RESOLVE EWI!!! /*** {code} - {message} ***/!!!"
    
    def __init__(self, 
                 external_volume: str = "<EXTERNAL_VOLUME>",
                 base_location_pattern: str = "{schema}/{table}",
                 include_comments: bool = True,
                 include_ewi: bool = True):
        """
        Initialize converter with configuration.
        
        Args:
            external_volume: Name of the Snowflake external volume
            base_location_pattern: Pattern for base location (supports {schema}, {table})
            include_comments: Include comments for constraints
            include_ewi: Include EWI markers in output
        """
        self.external_volume = external_volume
        self.base_location_pattern = base_location_pattern
        self.include_comments = include_comments
        self.include_ewi = include_ewi
        
        self.parser = DB2Parser()
        self.mapper = DataTypeMapper()
        self.assessor = Assessor()
    
    def convert(self, ddl: str) -> ConversionResult:
        """
        Convert DB2 DDL to Snowflake Iceberg DDL.
        
        Args:
            ddl: DB2 DDL string (one or more CREATE TABLE statements)
        
        Returns:
            ConversionResult with Iceberg DDL and assessment
        """
        result = ConversionResult(
            iceberg_ddl="",
            assessment=AssessmentReport()
        )
        
        # First assess the DDL
        result.assessment = self.assessor.assess(ddl)
        
        # Parse tables
        tables = self.parser.parse(ddl)
        
        if not tables:
            result.success = False
            result.error_message = "No valid CREATE TABLE statements found"
            return result
        
        # Convert each table
        converted_statements = []
        ewi_count = 0
        
        for table in tables:
            stmt, ewis = self._convert_table(table)
            converted_statements.append(stmt)
            ewi_count += ewis
        
        result.iceberg_ddl = "\n\n".join(converted_statements)
        result.ewi_count = ewi_count
        result.tables_converted = len(tables)
        
        return result
    
    def _convert_table(self, table: TableDefinition) -> tuple[str, int]:
        """
        Convert a single table definition to Iceberg DDL.
        
        Handles special table types:
        - VOLATILE tables: Convert to Snowflake TEMPORARY (session-scoped)
        - GLOBAL TEMPORARY tables: Convert to Snowflake TEMPORARY
        
        Returns:
            Tuple of (DDL string, EWI count)
        """
        # Handle VOLATILE and GLOBAL TEMPORARY tables specially
        if table.volatile or table.global_temporary:
            return self._convert_temp_table(table)
        
        lines = []
        ewi_count = 0
        
        # Table header comment
        if self.include_comments:
            lines.append(f"-- Converted from DB2: {table.full_name}")
            if table.editproc:
                lines.append(f"-- WARNING: Original table had EDITPROC: {table.editproc}")
            if table.validproc:
                lines.append(f"-- WARNING: Original table had VALIDPROC: {table.validproc}")
        
        # CREATE ICEBERG TABLE statement
        table_name = self._format_identifier(table.full_name)
        lines.append(f"CREATE OR REPLACE ICEBERG TABLE {table_name} (")
        
        # Convert columns
        column_lines = []
        for i, col in enumerate(table.columns):
            col_line, col_ewis = self._convert_column(col, table.full_name)
            ewi_count += col_ewis
            
            # Add comma except for last column (before constraints)
            if i < len(table.columns) - 1 or self._has_pk_constraint(table.constraints):
                col_line += ","
            
            column_lines.append(col_line)
        
        # Add primary key constraint if exists
        pk_constraint = self._get_pk_constraint(table.constraints)
        if pk_constraint:
            pk_cols = ", ".join(self._format_identifier(c) for c in pk_constraint.columns)
            pk_line = f"    PRIMARY KEY ({pk_cols})"
            column_lines.append(pk_line)
        
        lines.extend(column_lines)
        lines.append(")")
        
        # Iceberg-specific clauses
        lines.append("CATALOG = 'SNOWFLAKE'")
        lines.append(f"EXTERNAL_VOLUME = '{self.external_volume}'")
        
        # Generate base location
        base_location = self._generate_base_location(table)
        lines.append(f"BASE_LOCATION = '{base_location}'")
        
        # Add constraint comments
        if self.include_comments:
            constraint_comments = self._generate_constraint_comments(table.constraints)
            if constraint_comments:
                lines.append("")
                lines.extend(constraint_comments)
        
        # End statement
        lines.append(";")
        
        return "\n".join(lines), ewi_count
    
    def _convert_temp_table(self, table: TableDefinition) -> tuple[str, int]:
        """
        Convert VOLATILE or GLOBAL TEMPORARY table to Snowflake TEMPORARY table.
        
        DB2 VOLATILE/GLOBAL TEMPORARY tables are session-scoped and cannot be
        converted to Iceberg (which doesn't support temporary tables).
        
        Returns:
            Tuple of (DDL string, EWI count)
        """
        lines = []
        ewi_count = 0
        
        # Determine the original table type for documentation
        original_type = "VOLATILE" if table.volatile else "GLOBAL TEMPORARY"
        
        # Header comment
        if self.include_comments:
            lines.append(f"-- Converted from DB2 {original_type} table: {table.full_name}")
            lines.append(f"-- Kept as Snowflake TEMPORARY (Iceberg doesn't support temporary tables)")
            lines.append(f"-- Table will remain session-scoped as originally intended")
        
        # CREATE TEMPORARY TABLE (not Iceberg)
        table_name = self._format_identifier(table.full_name)
        lines.append(f"CREATE OR REPLACE TEMPORARY TABLE {table_name} (")
        
        # Convert columns with type mapping (but keep as standard Snowflake, not Iceberg)
        column_lines = []
        for i, col in enumerate(table.columns):
            col_line, col_ewis = self._convert_column(col, table.full_name)
            ewi_count += col_ewis
            
            # Add comma
            if i < len(table.columns) - 1 or self._has_pk_constraint(table.constraints):
                col_line += ","
            
            column_lines.append(col_line)
        
        # Add primary key
        pk_constraint = self._get_pk_constraint(table.constraints)
        if pk_constraint:
            pk_cols = ", ".join(self._format_identifier(c) for c in pk_constraint.columns)
            column_lines.append(f"    PRIMARY KEY ({pk_cols})")
        
        lines.extend(column_lines)
        lines.append(");")
        
        # Add EWI marker for temporary table
        if self.include_ewi:
            ewi_msg = self.EWI_TEMPLATE.format(
                code="SSC-EWI-DB2ICE-0030",
                message=f"{original_type} table kept as Snowflake TEMPORARY - Iceberg doesn't support temporary tables"
            )
            lines.append("")
            lines.append(f"-- {ewi_msg}")
            ewi_count += 1
        
        return "\n".join(lines), ewi_count
    
    def _convert_column(self, col: Column, table_name: str) -> tuple[str, int]:
        """
        Convert a column definition.
        
        Returns:
            Tuple of (column DDL line, EWI count)
        """
        ewi_count = 0
        parts = []
        ewi_markers = []
        
        # Column name
        col_name = self._format_identifier(col.name)
        parts.append(f"    {col_name}")
        
        # Map data type
        mapping = self.mapper.map_type(
            col.data_type,
            col.length,
            col.precision,
            col.scale,
            col.for_bit_data,
            col.ccsid
        )
        
        parts.append(mapping.target_type)
        
        # Add EWI if needed
        if mapping.ewi_code and self.include_ewi:
            if mapping.status == ConversionStatus.UNSUPPORTED:
                ewi_markers.append(self._format_ewi(mapping.ewi_code, mapping.ewi_message))
                ewi_count += 1
            elif mapping.status == ConversionStatus.LOSSY:
                ewi_markers.append(self._format_ewi(mapping.ewi_code, mapping.ewi_message))
                ewi_count += 1
        
        # NOT NULL constraint
        if not col.nullable:
            parts.append("NOT NULL")
        
        # Check for FIELDPROC
        if col.fieldproc and self.include_ewi:
            ewi_markers.append(self._format_ewi(
                'SSC-EWI-DB2ICE-0011',
                f'FIELDPROC {col.fieldproc} - data may be encrypted/transformed'
            ))
            ewi_count += 1
        
        # Check for GENERATED
        if col.generated and self.include_ewi:
            ewi_markers.append(self._format_ewi(
                'SSC-EWI-DB2ICE-0014',
                f'GENERATED {col.generated} not supported in Iceberg'
            ))
            ewi_count += 1
        
        # Build the line
        line = " ".join(parts)
        
        # Add EWI markers
        if ewi_markers:
            line += "\n" + "\n".join(f"        {ewi}" for ewi in ewi_markers)
        
        return line, ewi_count
    
    def _format_ewi(self, code: str, message: str) -> str:
        """Format an EWI marker"""
        return self.EWI_TEMPLATE.format(code=code, message=message)
    
    def _format_identifier(self, identifier: str) -> str:
        """Format an identifier (quote if needed)"""
        # Check if identifier needs quoting
        if self._needs_quoting(identifier):
            return f'"{identifier}"'
        return identifier.upper()
    
    def _needs_quoting(self, identifier: str) -> bool:
        """Check if identifier needs to be quoted"""
        # Reserved words, special characters, or mixed case
        reserved = {'ORDER', 'GROUP', 'SELECT', 'FROM', 'WHERE', 'TABLE', 'INDEX', 
                   'CREATE', 'DROP', 'ALTER', 'INSERT', 'UPDATE', 'DELETE', 'VALUES',
                   'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'DATE', 'TIME', 'TIMESTAMP'}
        
        upper = identifier.upper()
        if upper in reserved:
            return True
        
        # Check for special characters or spaces
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
            return True
        
        return False
    
    def _generate_base_location(self, table: TableDefinition) -> str:
        """Generate base location from pattern"""
        location = self.base_location_pattern
        
        schema = table.schema if table.schema else "default"
        location = location.replace("{schema}", schema.lower())
        location = location.replace("{table}", table.name.lower())
        
        return location
    
    def _has_pk_constraint(self, constraints: list[Constraint]) -> bool:
        """Check if there's a primary key constraint"""
        return any(c.type == 'PRIMARY KEY' for c in constraints)
    
    def _get_pk_constraint(self, constraints: list[Constraint]) -> Optional[Constraint]:
        """Get the primary key constraint if exists"""
        for c in constraints:
            if c.type == 'PRIMARY KEY':
                return c
        return None
    
    def _generate_constraint_comments(self, constraints: list[Constraint]) -> list[str]:
        """Generate comments for non-PK constraints"""
        comments = []
        
        for constraint in constraints:
            if constraint.type == 'PRIMARY KEY':
                continue  # Already handled in column definitions
            
            if constraint.type == 'FOREIGN KEY':
                ref_cols = ", ".join(constraint.reference_columns)
                cols = ", ".join(constraint.columns)
                name = f" {constraint.name}" if constraint.name else ""
                comments.append(
                    f"-- FOREIGN KEY{name}: ({cols}) REFERENCES {constraint.reference_table}({ref_cols})"
                )
                comments.append("-- NOTE: Foreign keys are not enforced in Iceberg tables")
            
            elif constraint.type == 'UNIQUE':
                cols = ", ".join(constraint.columns)
                name = f" {constraint.name}" if constraint.name else ""
                comments.append(f"-- UNIQUE{name}: ({cols})")
                comments.append("-- NOTE: UNIQUE constraints are not enforced in Iceberg tables")
            
            elif constraint.type == 'CHECK':
                name = f" {constraint.name}" if constraint.name else ""
                comments.append(f"-- CHECK{name}: {constraint.check_condition}")
                comments.append("-- NOTE: CHECK constraints are not enforced in Iceberg tables")
        
        return comments


def convert_ddl(ddl: str, 
                external_volume: str = "<EXTERNAL_VOLUME>",
                base_location: str = "{schema}/{table}") -> ConversionResult:
    """
    Convenience function to convert DDL.
    Used by Streamlit app.
    """
    converter = DB2IceConverter(
        external_volume=external_volume,
        base_location_pattern=base_location
    )
    return converter.convert(ddl)
