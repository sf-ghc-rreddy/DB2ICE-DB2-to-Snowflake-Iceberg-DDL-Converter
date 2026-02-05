"""
Assessment Engine - Analyzes DB2 DDL and generates readiness scores
Similar to SMA (Snowflake Migration Assessment) style reports
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
import json

from db2ice.parser import DB2Parser, TableDefinition, Column, Constraint
from db2ice.mapper import DataTypeMapper, ConversionStatus


class ReadinessLevel(Enum):
    """Traffic light readiness levels"""
    GREEN = "green"    # >= 80% - Ready to convert
    YELLOW = "yellow"  # 50-79% - Review recommended
    RED = "red"        # < 50% - Significant issues


class IssueSeverity(Enum):
    """Severity levels for issues"""
    CRITICAL = "critical"  # Blocks conversion
    WARNING = "warning"    # Review recommended
    INFO = "info"          # Informational


@dataclass
class Issue:
    """Represents an assessment issue"""
    code: str
    severity: IssueSeverity
    message: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    suggestion: Optional[str] = None


@dataclass
class TableAssessment:
    """Assessment results for a single table"""
    table_name: str
    schema: Optional[str] = None
    column_count: int = 0
    constraint_count: int = 0
    readiness_score: float = 100.0
    readiness_level: ReadinessLevel = ReadinessLevel.GREEN
    can_auto_convert: bool = True
    issues: list = field(default_factory=list)
    type_distribution: dict = field(default_factory=dict)
    
    @property
    def full_name(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name


@dataclass
class AssessmentReport:
    """Complete assessment report for all tables"""
    # Summary metrics
    tables_total: int = 0
    tables_auto: int = 0      # Can auto-convert
    tables_manual: int = 0    # Need manual review
    tables_blocked: int = 0   # Critical issues
    
    # Readiness scores
    overall_score: float = 0.0
    overall_level: ReadinessLevel = ReadinessLevel.GREEN
    datatype_score: float = 0.0
    constraint_score: float = 0.0
    partition_score: float = 0.0
    special_features_score: float = 0.0
    
    # Inventory
    total_columns: int = 0
    total_constraints: int = 0
    
    # Issues
    critical_issues: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    info_items: list = field(default_factory=list)
    
    # Per-table assessments
    table_assessments: list = field(default_factory=list)
    
    # Type distribution across all tables
    type_distribution: dict = field(default_factory=dict)
    
    # Feature usage
    features_used: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'summary': {
                'tables_total': self.tables_total,
                'tables_auto_convert': self.tables_auto,
                'tables_manual_review': self.tables_manual,
                'tables_blocked': self.tables_blocked,
            },
            'readiness': {
                'overall_score': round(self.overall_score, 1),
                'overall_level': self.overall_level.value,
                'datatype_score': round(self.datatype_score, 1),
                'constraint_score': round(self.constraint_score, 1),
                'partition_score': round(self.partition_score, 1),
                'special_features_score': round(self.special_features_score, 1),
            },
            'inventory': {
                'total_columns': self.total_columns,
                'total_constraints': self.total_constraints,
            },
            'issues': {
                'critical': [self._issue_to_dict(i) for i in self.critical_issues],
                'warnings': [self._issue_to_dict(i) for i in self.warnings],
                'info': [self._issue_to_dict(i) for i in self.info_items],
            },
            'type_distribution': self.type_distribution,
            'features_used': self.features_used,
            'tables': [self._table_assessment_to_dict(t) for t in self.table_assessments],
        }
    
    def _issue_to_dict(self, issue: Issue) -> dict:
        return {
            'code': issue.code,
            'severity': issue.severity.value,
            'message': issue.message,
            'table': issue.table_name,
            'column': issue.column_name,
            'suggestion': issue.suggestion,
        }
    
    def _table_assessment_to_dict(self, ta: TableAssessment) -> dict:
        return {
            'name': ta.full_name,
            'columns': ta.column_count,
            'constraints': ta.constraint_count,
            'score': round(ta.readiness_score, 1),
            'level': ta.readiness_level.value,
            'can_auto_convert': ta.can_auto_convert,
            'issues': [self._issue_to_dict(i) for i in ta.issues],
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=indent)


class Assessor:
    """
    Assesses DB2 DDL for Iceberg conversion readiness.
    Generates SMA-style reports with traffic light scoring.
    """
    
    # Score weights for different categories
    WEIGHTS = {
        'datatype': 0.40,      # Data type compatibility
        'constraint': 0.20,   # Constraint support
        'partition': 0.15,    # Partition compatibility
        'special': 0.25,      # Special features (EDITPROC, etc.)
    }
    
    # Penalties for various issues
    PENALTIES = {
        'unsupported_type': 25,      # Per unsupported type
        'lossy_conversion': 10,      # Per lossy conversion
        'compatible_type': 2,        # Minor compatibility issue
        'editproc': 50,              # EDITPROC (critical)
        'validproc': 40,             # VALIDPROC (critical)
        'fieldproc': 50,             # FIELDPROC (critical)
        'xml_column': 30,            # XML column
        'foreign_key': 5,            # FK not enforced in Iceberg
        'check_constraint': 5,       # CHECK not enforced
        'complex_partition': 20,     # Complex partitioning
        'generated_column': 15,      # Generated columns
        'large_lob': 10,             # LOB size concerns
    }
    
    def __init__(self):
        self.parser = DB2Parser()
        self.mapper = DataTypeMapper()
    
    def assess(self, ddl: str) -> AssessmentReport:
        """
        Assess DB2 DDL and generate readiness report.
        
        Args:
            ddl: DB2 DDL string (one or more CREATE TABLE statements)
        
        Returns:
            AssessmentReport with scores and issues
        """
        report = AssessmentReport()
        
        # Parse the DDL
        tables = self.parser.parse(ddl)
        
        if not tables:
            report.critical_issues.append(Issue(
                code='SSC-EWI-DB2ICE-0000',
                severity=IssueSeverity.CRITICAL,
                message='No valid CREATE TABLE statements found in input'
            ))
            return report
        
        report.tables_total = len(tables)
        
        # Assess each table
        datatype_scores = []
        constraint_scores = []
        partition_scores = []
        special_scores = []
        
        for table in tables:
            ta = self._assess_table(table)
            report.table_assessments.append(ta)
            
            # Aggregate metrics
            report.total_columns += ta.column_count
            report.total_constraints += ta.constraint_count
            
            # Aggregate type distribution
            for dtype, count in ta.type_distribution.items():
                report.type_distribution[dtype] = report.type_distribution.get(dtype, 0) + count
            
            # Collect all issues
            for issue in ta.issues:
                if issue.severity == IssueSeverity.CRITICAL:
                    report.critical_issues.append(issue)
                elif issue.severity == IssueSeverity.WARNING:
                    report.warnings.append(issue)
                else:
                    report.info_items.append(issue)
            
            # Categorize tables
            if not ta.can_auto_convert:
                if any(i.severity == IssueSeverity.CRITICAL for i in ta.issues):
                    report.tables_blocked += 1
                else:
                    report.tables_manual += 1
            else:
                report.tables_auto += 1
            
            # Collect component scores
            scores = self._calculate_component_scores(ta)
            datatype_scores.append(scores['datatype'])
            constraint_scores.append(scores['constraint'])
            partition_scores.append(scores['partition'])
            special_scores.append(scores['special'])
        
        # Calculate aggregate scores
        report.datatype_score = sum(datatype_scores) / len(datatype_scores) if datatype_scores else 100
        report.constraint_score = sum(constraint_scores) / len(constraint_scores) if constraint_scores else 100
        report.partition_score = sum(partition_scores) / len(partition_scores) if partition_scores else 100
        report.special_features_score = sum(special_scores) / len(special_scores) if special_scores else 100
        
        # Calculate overall score
        report.overall_score = (
            report.datatype_score * self.WEIGHTS['datatype'] +
            report.constraint_score * self.WEIGHTS['constraint'] +
            report.partition_score * self.WEIGHTS['partition'] +
            report.special_features_score * self.WEIGHTS['special']
        )
        
        # Determine overall level
        report.overall_level = self._score_to_level(report.overall_score)
        
        # Track feature usage
        report.features_used = self._aggregate_features(tables)
        
        return report
    
    def _assess_table(self, table: TableDefinition) -> TableAssessment:
        """Assess a single table"""
        ta = TableAssessment(
            table_name=table.name,
            schema=table.schema,
            column_count=len(table.columns),
            constraint_count=len(table.constraints)
        )
        
        penalties = 0
        max_penalty = 100
        
        # Assess columns and data types
        for col in table.columns:
            # Track type distribution
            base_type = col.data_type.split('(')[0].strip()
            ta.type_distribution[base_type] = ta.type_distribution.get(base_type, 0) + 1
            
            # Map the type
            mapping = self.mapper.map_type(
                col.data_type,
                col.length,
                col.precision,
                col.scale,
                col.for_bit_data,
                col.ccsid
            )
            
            # Check conversion status
            if mapping.status == ConversionStatus.UNSUPPORTED:
                penalties += self.PENALTIES['unsupported_type']
                ta.can_auto_convert = False
                ta.issues.append(Issue(
                    code=mapping.ewi_code or 'SSC-EWI-DB2ICE-0099',
                    severity=IssueSeverity.CRITICAL,
                    message=mapping.ewi_message or f'Unsupported type: {col.data_type}',
                    table_name=table.full_name,
                    column_name=col.name,
                    suggestion='Manual conversion required - consider alternative data model'
                ))
            elif mapping.status == ConversionStatus.LOSSY:
                penalties += self.PENALTIES['lossy_conversion']
                ta.issues.append(Issue(
                    code=mapping.ewi_code or 'SSC-EWI-DB2ICE-0098',
                    severity=IssueSeverity.WARNING,
                    message=mapping.ewi_message or f'Lossy conversion: {col.data_type}',
                    table_name=table.full_name,
                    column_name=col.name,
                    suggestion='Review data to ensure no precision/data loss'
                ))
            elif mapping.status == ConversionStatus.COMPATIBLE and mapping.ewi_code:
                penalties += self.PENALTIES['compatible_type']
                ta.issues.append(Issue(
                    code=mapping.ewi_code,
                    severity=IssueSeverity.INFO,
                    message=mapping.ewi_message,
                    table_name=table.full_name,
                    column_name=col.name
                ))
            
            # Check FIELDPROC
            if col.fieldproc:
                penalties += self.PENALTIES['fieldproc']
                ta.can_auto_convert = False
                ta.issues.append(Issue(
                    code='SSC-EWI-DB2ICE-0011',
                    severity=IssueSeverity.CRITICAL,
                    message=f'FIELDPROC {col.fieldproc} - column data may be encrypted/transformed',
                    table_name=table.full_name,
                    column_name=col.name,
                    suggestion='Review FIELDPROC logic - data transformation required before migration'
                ))
            
            # Check GENERATED columns
            if col.generated:
                penalties += self.PENALTIES['generated_column']
                ta.issues.append(Issue(
                    code='SSC-EWI-DB2ICE-0014',
                    severity=IssueSeverity.WARNING,
                    message=f'GENERATED {col.generated} column - Iceberg does not support generated columns',
                    table_name=table.full_name,
                    column_name=col.name,
                    suggestion='Remove GENERATED clause or compute values during ETL'
                ))
        
        # Assess constraints
        for constraint in table.constraints:
            if constraint.type == 'FOREIGN KEY':
                penalties += self.PENALTIES['foreign_key']
                ta.issues.append(Issue(
                    code='SSC-EWI-DB2ICE-0016',
                    severity=IssueSeverity.INFO,
                    message=f'Foreign key constraint - not enforced in Iceberg tables',
                    table_name=table.full_name,
                    suggestion='Foreign key will be documented but not enforced'
                ))
            elif constraint.type == 'CHECK':
                penalties += self.PENALTIES['check_constraint']
                ta.issues.append(Issue(
                    code='SSC-EWI-DB2ICE-0015',
                    severity=IssueSeverity.INFO,
                    message=f'CHECK constraint - not enforced in Iceberg tables',
                    table_name=table.full_name,
                    suggestion='CHECK constraint will be documented but not enforced'
                ))
        
        # Assess EDITPROC
        if table.editproc:
            penalties += self.PENALTIES['editproc']
            ta.can_auto_convert = False
            ta.issues.append(Issue(
                code='SSC-EWI-DB2ICE-0012',
                severity=IssueSeverity.CRITICAL,
                message=f'EDITPROC {table.editproc} - table uses edit procedure for data transformation',
                table_name=table.full_name,
                suggestion='Review EDITPROC logic - data may require transformation before migration'
            ))
        
        # Assess VALIDPROC
        if table.validproc:
            penalties += self.PENALTIES['validproc']
            ta.can_auto_convert = False
            ta.issues.append(Issue(
                code='SSC-EWI-DB2ICE-0013',
                severity=IssueSeverity.CRITICAL,
                message=f'VALIDPROC {table.validproc} - table uses validation procedure',
                table_name=table.full_name,
                suggestion='Implement validation logic in application layer or Snowflake procedures'
            ))
        
        # Assess partitioning
        if table.partition:
            if table.partition.type == 'HASH':
                penalties += self.PENALTIES['complex_partition']
                ta.issues.append(Issue(
                    code='SSC-EWI-DB2ICE-0017',
                    severity=IssueSeverity.WARNING,
                    message='HASH partitioning not directly supported - will be removed',
                    table_name=table.full_name,
                    suggestion='Iceberg uses automatic micro-partitioning'
                ))
            elif table.partition.type == 'RANGE':
                ta.issues.append(Issue(
                    code='SSC-EWI-DB2ICE-0017',
                    severity=IssueSeverity.INFO,
                    message='RANGE partitioning will be removed - Iceberg uses automatic partitioning',
                    table_name=table.full_name,
                    suggestion='Consider Iceberg partition transforms if needed'
                ))
        
        # Calculate readiness score
        ta.readiness_score = max(0, 100 - penalties)
        ta.readiness_level = self._score_to_level(ta.readiness_score)
        
        return ta
    
    def _calculate_component_scores(self, ta: TableAssessment) -> dict:
        """Calculate component scores for a table assessment"""
        scores = {
            'datatype': 100.0,
            'constraint': 100.0,
            'partition': 100.0,
            'special': 100.0,
        }
        
        for issue in ta.issues:
            code = issue.code
            
            # Categorize by issue type
            if 'DATATYPE' in code or code in ['SSC-EWI-DB2ICE-0001', 'SSC-EWI-DB2ICE-0002', 
                                               'SSC-EWI-DB2ICE-0003', 'SSC-EWI-DB2ICE-0004',
                                               'SSC-EWI-DB2ICE-0005', 'SSC-EWI-DB2ICE-0006',
                                               'SSC-EWI-DB2ICE-0007', 'SSC-EWI-DB2ICE-0008',
                                               'SSC-EWI-DB2ICE-0009', 'SSC-EWI-DB2ICE-0010']:
                penalty = 5 if issue.severity == IssueSeverity.INFO else 15 if issue.severity == IssueSeverity.WARNING else 30
                scores['datatype'] = max(0, scores['datatype'] - penalty)
            
            elif code in ['SSC-EWI-DB2ICE-0015', 'SSC-EWI-DB2ICE-0016']:
                penalty = 5 if issue.severity == IssueSeverity.INFO else 10
                scores['constraint'] = max(0, scores['constraint'] - penalty)
            
            elif code == 'SSC-EWI-DB2ICE-0017':
                penalty = 10 if issue.severity == IssueSeverity.INFO else 20
                scores['partition'] = max(0, scores['partition'] - penalty)
            
            elif code in ['SSC-EWI-DB2ICE-0011', 'SSC-EWI-DB2ICE-0012', 'SSC-EWI-DB2ICE-0013', 'SSC-EWI-DB2ICE-0014']:
                penalty = 10 if issue.severity == IssueSeverity.INFO else 25 if issue.severity == IssueSeverity.WARNING else 50
                scores['special'] = max(0, scores['special'] - penalty)
        
        return scores
    
    def _score_to_level(self, score: float) -> ReadinessLevel:
        """Convert score to traffic light level"""
        if score >= 80:
            return ReadinessLevel.GREEN
        elif score >= 50:
            return ReadinessLevel.YELLOW
        else:
            return ReadinessLevel.RED
    
    def _aggregate_features(self, tables: list[TableDefinition]) -> dict:
        """Aggregate feature usage across all tables"""
        features = {
            'editproc': 0,
            'validproc': 0,
            'fieldproc': 0,
            'partitioning': 0,
            'generated_columns': 0,
            'foreign_keys': 0,
            'check_constraints': 0,
            'xml_columns': 0,
            'graphic_columns': 0,
            'lob_columns': 0,
        }
        
        for table in tables:
            if table.editproc:
                features['editproc'] += 1
            if table.validproc:
                features['validproc'] += 1
            if table.partition:
                features['partitioning'] += 1
            
            for col in table.columns:
                if col.fieldproc:
                    features['fieldproc'] += 1
                if col.generated:
                    features['generated_columns'] += 1
                if col.data_type.upper() == 'XML':
                    features['xml_columns'] += 1
                if col.data_type.upper() in ('GRAPHIC', 'VARGRAPHIC', 'DBCLOB', 'LONG VARGRAPHIC'):
                    features['graphic_columns'] += 1
                if col.data_type.upper() in ('CLOB', 'BLOB', 'DBCLOB'):
                    features['lob_columns'] += 1
            
            for constraint in table.constraints:
                if constraint.type == 'FOREIGN KEY':
                    features['foreign_keys'] += 1
                elif constraint.type == 'CHECK':
                    features['check_constraints'] += 1
        
        return features


def assess_ddl(ddl: str) -> dict:
    """
    Convenience function to assess DDL and return dictionary result.
    Used by Streamlit app.
    """
    assessor = Assessor()
    report = assessor.assess(ddl)
    return report.to_dict()
