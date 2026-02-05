"""
Data Type Mapper - Maps DB2 data types to Snowflake Iceberg compatible types
"""

from dataclasses import dataclass
from typing import Optional
from enum import Enum


class ConversionStatus(Enum):
    """Status of type conversion"""
    DIRECT = "direct"           # Direct mapping, no issues
    COMPATIBLE = "compatible"   # Compatible with minor changes
    LOSSY = "lossy"             # Conversion possible but with data loss risk
    UNSUPPORTED = "unsupported" # No Iceberg equivalent


@dataclass
class TypeMapping:
    """Result of a data type mapping"""
    source_type: str
    target_type: str
    status: ConversionStatus
    ewi_code: Optional[str] = None
    ewi_message: Optional[str] = None
    notes: Optional[str] = None


class DataTypeMapper:
    """
    Maps DB2 data types to Snowflake Iceberg compatible types.
    
    Iceberg Type Restrictions:
    - TIME/TIMESTAMP: Must be precision 6 (microseconds)
    - No CHAR (fixed length) - convert to VARCHAR
    - No XML type
    - VARCHAR max 16MB (Iceberg limit)
    - BINARY max 8MB
    - Structured types (ARRAY, OBJECT, MAP) supported
    """
    
    # Direct mappings (no transformation needed)
    DIRECT_MAPPINGS = {
        'SMALLINT': 'SMALLINT',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'REAL': 'FLOAT',
        'DOUBLE': 'DOUBLE',
        'DATE': 'DATE',
        'BOOLEAN': 'BOOLEAN',
    }
    
    # EWI Codes
    EWI_CODES = {
        'CHAR_TO_VARCHAR': 'SSC-EWI-DB2ICE-0001',
        'PRECISION_ADJUST': 'SSC-EWI-DB2ICE-0002',
        'TIME_PRECISION': 'SSC-EWI-DB2ICE-0003',
        'TIMESTAMP_PRECISION': 'SSC-EWI-DB2ICE-0004',
        'XML_UNSUPPORTED': 'SSC-EWI-DB2ICE-0005',
        'GRAPHIC_CONVERT': 'SSC-EWI-DB2ICE-0006',
        'DECFLOAT_CONVERT': 'SSC-EWI-DB2ICE-0007',
        'LOB_SIZE_LIMIT': 'SSC-EWI-DB2ICE-0008',
        'ROWID_CONVERT': 'SSC-EWI-DB2ICE-0009',
        'FOR_BIT_DATA': 'SSC-EWI-DB2ICE-0010',
        'FIELDPROC': 'SSC-EWI-DB2ICE-0011',
        'EDITPROC': 'SSC-EWI-DB2ICE-0012',
        'VALIDPROC': 'SSC-EWI-DB2ICE-0013',
        'GENERATED_COL': 'SSC-EWI-DB2ICE-0014',
        'CHECK_CONSTRAINT': 'SSC-EWI-DB2ICE-0015',
        'FOREIGN_KEY': 'SSC-EWI-DB2ICE-0016',
        'PARTITION_COMPLEX': 'SSC-EWI-DB2ICE-0017',
        'CCSID_ENCODING': 'SSC-EWI-DB2ICE-0018',
        'LONG_VARCHAR': 'SSC-EWI-DB2ICE-0019',
        'BINARY_CONVERT': 'SSC-EWI-DB2ICE-0020',
    }
    
    # Maximum sizes for Iceberg
    MAX_VARCHAR_SIZE = 16 * 1024 * 1024  # 16MB
    MAX_BINARY_SIZE = 8 * 1024 * 1024     # 8MB
    MAX_LOB_SIZE = 128 * 1024 * 1024      # 128MB (Snowflake limit)
    
    def __init__(self):
        self.warnings = []
        self.errors = []
    
    def map_type(self, db2_type: str, length: Optional[int] = None,
                 precision: Optional[int] = None, scale: Optional[int] = None,
                 for_bit_data: bool = False, ccsid: Optional[str] = None) -> TypeMapping:
        """
        Map a DB2 data type to Snowflake Iceberg compatible type.
        
        Args:
            db2_type: The DB2 data type name
            length: Length for character/binary types
            precision: Precision for numeric types
            scale: Scale for decimal types
            for_bit_data: Whether FOR BIT DATA is specified
            ccsid: Character set encoding
        
        Returns:
            TypeMapping with target type and any EWI information
        """
        db2_type = db2_type.upper().strip()
        
        # Handle FOR BIT DATA - convert to BINARY
        if for_bit_data:
            return self._map_for_bit_data(db2_type, length)
        
        # Direct mappings
        if db2_type in self.DIRECT_MAPPINGS:
            return TypeMapping(
                source_type=db2_type,
                target_type=self.DIRECT_MAPPINGS[db2_type],
                status=ConversionStatus.DIRECT
            )
        
        # Character types
        if db2_type in ('CHAR', 'CHARACTER'):
            return self._map_char(length)
        
        if db2_type in ('VARCHAR', 'CHAR VARYING', 'CHARACTER VARYING'):
            return self._map_varchar(length)
        
        if db2_type == 'LONG VARCHAR':
            return self._map_long_varchar()
        
        if db2_type == 'CLOB':
            return self._map_clob(length)
        
        # Numeric types
        if db2_type in ('DECIMAL', 'DEC', 'NUMERIC'):
            return self._map_decimal(precision, scale)
        
        if db2_type == 'FLOAT':
            return self._map_float(precision)
        
        if db2_type == 'DECFLOAT':
            return self._map_decfloat(precision)
        
        # Date/Time types
        if db2_type == 'TIME':
            return self._map_time(precision)
        
        if db2_type == 'TIMESTAMP':
            return self._map_timestamp(precision)
        
        # Binary types
        if db2_type == 'BINARY':
            return self._map_binary(length)
        
        if db2_type in ('VARBINARY', 'BINARY VARYING'):
            return self._map_varbinary(length)
        
        if db2_type == 'BLOB':
            return self._map_blob(length)
        
        # Graphic types (DBCS)
        if db2_type == 'GRAPHIC':
            return self._map_graphic(length)
        
        if db2_type == 'VARGRAPHIC':
            return self._map_vargraphic(length)
        
        if db2_type == 'LONG VARGRAPHIC':
            return self._map_long_vargraphic()
        
        if db2_type == 'DBCLOB':
            return self._map_dbclob(length)
        
        # Special types
        if db2_type == 'XML':
            return self._map_xml()
        
        if db2_type == 'ROWID':
            return self._map_rowid()
        
        # Unknown type
        return TypeMapping(
            source_type=db2_type,
            target_type='VARCHAR',
            status=ConversionStatus.LOSSY,
            ewi_code='SSC-EWI-DB2ICE-0099',
            ewi_message=f'Unknown DB2 type {db2_type} converted to VARCHAR'
        )
    
    def _map_char(self, length: Optional[int]) -> TypeMapping:
        """CHAR -> VARCHAR (Iceberg doesn't support fixed-length CHAR)"""
        target_length = length if length else 1
        return TypeMapping(
            source_type=f'CHAR({length})' if length else 'CHAR',
            target_type=f'VARCHAR({target_length})',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['CHAR_TO_VARCHAR'],
            ewi_message='CHAR converted to VARCHAR - Iceberg does not support fixed-length CHAR',
            notes='Padding behavior may differ'
        )
    
    def _map_varchar(self, length: Optional[int]) -> TypeMapping:
        """VARCHAR -> VARCHAR"""
        if length and length > self.MAX_VARCHAR_SIZE:
            return TypeMapping(
                source_type=f'VARCHAR({length})',
                target_type='VARCHAR',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['LOB_SIZE_LIMIT'],
                ewi_message=f'VARCHAR({length}) exceeds Iceberg limit, using VARCHAR without length'
            )
        
        target = f'VARCHAR({length})' if length else 'VARCHAR'
        return TypeMapping(
            source_type=f'VARCHAR({length})' if length else 'VARCHAR',
            target_type=target,
            status=ConversionStatus.DIRECT
        )
    
    def _map_long_varchar(self) -> TypeMapping:
        """LONG VARCHAR -> VARCHAR"""
        return TypeMapping(
            source_type='LONG VARCHAR',
            target_type='VARCHAR',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['LONG_VARCHAR'],
            ewi_message='LONG VARCHAR converted to VARCHAR'
        )
    
    def _map_clob(self, length: Optional[int]) -> TypeMapping:
        """CLOB -> VARCHAR (with size warning if > 128MB)"""
        if length and length > self.MAX_LOB_SIZE:
            return TypeMapping(
                source_type=f'CLOB({length})',
                target_type='VARCHAR',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['LOB_SIZE_LIMIT'],
                ewi_message=f'CLOB size {length} exceeds Snowflake 128MB limit - data truncation may occur'
            )
        
        return TypeMapping(
            source_type=f'CLOB({length})' if length else 'CLOB',
            target_type='VARCHAR',
            status=ConversionStatus.COMPATIBLE,
            notes='CLOB converted to VARCHAR'
        )
    
    def _map_decimal(self, precision: Optional[int], scale: Optional[int]) -> TypeMapping:
        """DECIMAL -> NUMBER"""
        # DB2 DECIMAL max precision is 31, Snowflake NUMBER max is 38
        p = precision if precision else 5
        s = scale if scale else 0
        
        # Adjust if needed
        if p > 38:
            return TypeMapping(
                source_type=f'DECIMAL({precision},{scale})',
                target_type=f'NUMBER(38,{min(s, 37)})',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['PRECISION_ADJUST'],
                ewi_message=f'Precision {precision} exceeds maximum 38, adjusted to 38'
            )
        
        return TypeMapping(
            source_type=f'DECIMAL({p},{s})',
            target_type=f'NUMBER({p},{s})',
            status=ConversionStatus.DIRECT
        )
    
    def _map_float(self, precision: Optional[int]) -> TypeMapping:
        """FLOAT -> FLOAT or DOUBLE"""
        if precision and precision > 24:
            return TypeMapping(
                source_type=f'FLOAT({precision})',
                target_type='DOUBLE',
                status=ConversionStatus.DIRECT
            )
        return TypeMapping(
            source_type=f'FLOAT({precision})' if precision else 'FLOAT',
            target_type='FLOAT',
            status=ConversionStatus.DIRECT
        )
    
    def _map_decfloat(self, precision: Optional[int]) -> TypeMapping:
        """DECFLOAT -> DOUBLE (with precision warning)"""
        return TypeMapping(
            source_type=f'DECFLOAT({precision})' if precision else 'DECFLOAT',
            target_type='DOUBLE',
            status=ConversionStatus.LOSSY,
            ewi_code=self.EWI_CODES['DECFLOAT_CONVERT'],
            ewi_message='DECFLOAT converted to DOUBLE - decimal floating point precision may be lost'
        )
    
    def _map_time(self, precision: Optional[int]) -> TypeMapping:
        """TIME -> TIME(6) - Iceberg requires precision 6"""
        source_precision = precision if precision else 0
        if source_precision != 6:
            return TypeMapping(
                source_type=f'TIME({source_precision})' if precision else 'TIME',
                target_type='TIME(6)',
                status=ConversionStatus.COMPATIBLE,
                ewi_code=self.EWI_CODES['TIME_PRECISION'],
                ewi_message='TIME precision adjusted to 6 (microseconds) for Iceberg compatibility'
            )
        return TypeMapping(
            source_type='TIME(6)',
            target_type='TIME(6)',
            status=ConversionStatus.DIRECT
        )
    
    def _map_timestamp(self, precision: Optional[int]) -> TypeMapping:
        """TIMESTAMP -> TIMESTAMP_NTZ(6) - Iceberg requires precision 6"""
        source_precision = precision if precision else 6
        if source_precision != 6:
            return TypeMapping(
                source_type=f'TIMESTAMP({source_precision})',
                target_type='TIMESTAMP_NTZ(6)',
                status=ConversionStatus.COMPATIBLE,
                ewi_code=self.EWI_CODES['TIMESTAMP_PRECISION'],
                ewi_message='TIMESTAMP precision adjusted to 6 (microseconds) for Iceberg compatibility'
            )
        return TypeMapping(
            source_type=f'TIMESTAMP({source_precision})',
            target_type='TIMESTAMP_NTZ(6)',
            status=ConversionStatus.DIRECT
        )
    
    def _map_binary(self, length: Optional[int]) -> TypeMapping:
        """BINARY -> BINARY"""
        if length and length > self.MAX_BINARY_SIZE:
            return TypeMapping(
                source_type=f'BINARY({length})',
                target_type='BINARY',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['LOB_SIZE_LIMIT'],
                ewi_message=f'BINARY({length}) exceeds Iceberg limit'
            )
        return TypeMapping(
            source_type=f'BINARY({length})' if length else 'BINARY',
            target_type=f'BINARY({length})' if length else 'BINARY',
            status=ConversionStatus.DIRECT
        )
    
    def _map_varbinary(self, length: Optional[int]) -> TypeMapping:
        """VARBINARY -> VARBINARY"""
        if length and length > self.MAX_BINARY_SIZE:
            return TypeMapping(
                source_type=f'VARBINARY({length})',
                target_type='VARBINARY',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['LOB_SIZE_LIMIT'],
                ewi_message=f'VARBINARY({length}) exceeds Iceberg limit'
            )
        return TypeMapping(
            source_type=f'VARBINARY({length})' if length else 'VARBINARY',
            target_type=f'VARBINARY({length})' if length else 'VARBINARY',
            status=ConversionStatus.DIRECT
        )
    
    def _map_blob(self, length: Optional[int]) -> TypeMapping:
        """BLOB -> BINARY (with size warning)"""
        if length and length > self.MAX_LOB_SIZE:
            return TypeMapping(
                source_type=f'BLOB({length})',
                target_type='BINARY',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['LOB_SIZE_LIMIT'],
                ewi_message=f'BLOB size {length} exceeds Snowflake limit - data truncation may occur'
            )
        return TypeMapping(
            source_type=f'BLOB({length})' if length else 'BLOB',
            target_type='BINARY',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['BINARY_CONVERT'],
            ewi_message='BLOB converted to BINARY'
        )
    
    def _map_graphic(self, length: Optional[int]) -> TypeMapping:
        """GRAPHIC -> VARCHAR (DBCS double-byte to Unicode)"""
        # GRAPHIC stores double-byte characters, length is in DBCS characters
        # Convert to VARCHAR with appropriate length (2x for worst case)
        target_length = (length * 4) if length else None
        return TypeMapping(
            source_type=f'GRAPHIC({length})' if length else 'GRAPHIC',
            target_type=f'VARCHAR({target_length})' if target_length else 'VARCHAR',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['GRAPHIC_CONVERT'],
            ewi_message='GRAPHIC (DBCS) converted to VARCHAR - verify character encoding'
        )
    
    def _map_vargraphic(self, length: Optional[int]) -> TypeMapping:
        """VARGRAPHIC -> VARCHAR"""
        target_length = (length * 4) if length else None
        return TypeMapping(
            source_type=f'VARGRAPHIC({length})' if length else 'VARGRAPHIC',
            target_type=f'VARCHAR({target_length})' if target_length else 'VARCHAR',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['GRAPHIC_CONVERT'],
            ewi_message='VARGRAPHIC (DBCS) converted to VARCHAR - verify character encoding'
        )
    
    def _map_long_vargraphic(self) -> TypeMapping:
        """LONG VARGRAPHIC -> VARCHAR"""
        return TypeMapping(
            source_type='LONG VARGRAPHIC',
            target_type='VARCHAR',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['GRAPHIC_CONVERT'],
            ewi_message='LONG VARGRAPHIC converted to VARCHAR - verify character encoding'
        )
    
    def _map_dbclob(self, length: Optional[int]) -> TypeMapping:
        """DBCLOB -> VARCHAR"""
        if length and length > self.MAX_LOB_SIZE:
            return TypeMapping(
                source_type=f'DBCLOB({length})',
                target_type='VARCHAR',
                status=ConversionStatus.LOSSY,
                ewi_code=self.EWI_CODES['LOB_SIZE_LIMIT'],
                ewi_message=f'DBCLOB size {length} exceeds Snowflake limit - data truncation may occur'
            )
        return TypeMapping(
            source_type=f'DBCLOB({length})' if length else 'DBCLOB',
            target_type='VARCHAR',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['GRAPHIC_CONVERT'],
            ewi_message='DBCLOB converted to VARCHAR - verify character encoding'
        )
    
    def _map_xml(self) -> TypeMapping:
        """XML -> UNSUPPORTED (no Iceberg equivalent)"""
        return TypeMapping(
            source_type='XML',
            target_type='VARCHAR',
            status=ConversionStatus.UNSUPPORTED,
            ewi_code=self.EWI_CODES['XML_UNSUPPORTED'],
            ewi_message='XML type not supported in Iceberg tables - manual conversion required'
        )
    
    def _map_rowid(self) -> TypeMapping:
        """ROWID -> VARCHAR (system-generated, cannot be migrated directly)"""
        return TypeMapping(
            source_type='ROWID',
            target_type='VARCHAR(40)',
            status=ConversionStatus.LOSSY,
            ewi_code=self.EWI_CODES['ROWID_CONVERT'],
            ewi_message='ROWID converted to VARCHAR - values will not be preserved during migration'
        )
    
    def _map_for_bit_data(self, db2_type: str, length: Optional[int]) -> TypeMapping:
        """Handle FOR BIT DATA modifier - convert to BINARY"""
        target_length = length if length else 1
        return TypeMapping(
            source_type=f'{db2_type}({length}) FOR BIT DATA' if length else f'{db2_type} FOR BIT DATA',
            target_type=f'BINARY({target_length})',
            status=ConversionStatus.COMPATIBLE,
            ewi_code=self.EWI_CODES['FOR_BIT_DATA'],
            ewi_message='FOR BIT DATA converted to BINARY type'
        )
    
    def get_all_mappings(self) -> dict:
        """Return a summary of all type mappings for documentation"""
        return {
            'numeric': {
                'SMALLINT': 'SMALLINT',
                'INTEGER/INT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'DECIMAL/DEC/NUMERIC': 'NUMBER(p,s)',
                'REAL': 'FLOAT',
                'FLOAT': 'FLOAT/DOUBLE',
                'DOUBLE': 'DOUBLE',
                'DECFLOAT': 'DOUBLE (lossy)',
            },
            'character': {
                'CHAR': 'VARCHAR (compatible)',
                'VARCHAR': 'VARCHAR',
                'LONG VARCHAR': 'VARCHAR',
                'CLOB': 'VARCHAR',
            },
            'graphic': {
                'GRAPHIC': 'VARCHAR (encoding conversion)',
                'VARGRAPHIC': 'VARCHAR',
                'LONG VARGRAPHIC': 'VARCHAR',
                'DBCLOB': 'VARCHAR',
            },
            'binary': {
                'BINARY': 'BINARY',
                'VARBINARY': 'VARBINARY',
                'BLOB': 'BINARY',
            },
            'datetime': {
                'DATE': 'DATE',
                'TIME': 'TIME(6)',
                'TIMESTAMP': 'TIMESTAMP_NTZ(6)',
            },
            'special': {
                'XML': 'UNSUPPORTED',
                'ROWID': 'VARCHAR(40) (lossy)',
                'BOOLEAN': 'BOOLEAN',
            }
        }
