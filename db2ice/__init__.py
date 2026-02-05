"""
DB2ICE - DB2 to Iceberg Conversion Engine
Converts DB2 DDL to Snowflake Managed Iceberg Table DDL
"""

__version__ = "0.1.0"
__author__ = "DB2ICE Team"

from db2ice.parser import DB2Parser
from db2ice.mapper import DataTypeMapper
from db2ice.assessor import Assessor
from db2ice.converter import DB2IceConverter

__all__ = ["DB2Parser", "DataTypeMapper", "Assessor", "DB2IceConverter"]
