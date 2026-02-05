"""
DB2ICE - DB2 to Iceberg DDL Converter
Premium, slick UI design
"""

import streamlit as st
import sys
import os
from io import BytesIO
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db2ice.assessor import Assessor, ReadinessLevel, IssueSeverity
from db2ice.converter import DB2IceConverter
from db2ice.snowflake_converter import SnowflakeToIcebergConverter, SnowflakeParser

# PDF Generation - with fallback for Snowflake deployment
try:
    from fpdf import FPDF
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    FPDF = object  # Placeholder base class


if PDF_AVAILABLE:
    class AssessmentPDF(FPDF):
        """Custom PDF for assessment reports"""
        
        def header(self):
            self.set_font('Helvetica', 'B', 20)
            self.set_text_color(99, 102, 241)  # Indigo
            self.cell(0, 10, 'DB2ICE Assessment Report', ln=True, align='C')
            self.set_font('Helvetica', '', 10)
            self.set_text_color(100, 116, 139)  # Slate
            self.cell(0, 6, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', ln=True, align='C')
            self.ln(10)
        
        def footer(self):
            self.set_y(-15)
            self.set_font('Helvetica', 'I', 8)
            self.set_text_color(148, 163, 184)
            self.cell(0, 10, f'Page {self.page_no()}/{{nb}} - DB2ICE by Snowflake Cortex Code', align='C')
else:
    AssessmentPDF = None


def generate_assessment_pdf(report) -> bytes:
    """Generate PDF from assessment report"""
    if not PDF_AVAILABLE:
        return None
    
    pdf = AssessmentPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Overall Score Section
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 10, 'Migration Readiness Score', ln=True)
    
    # Score box
    if report.overall_level == ReadinessLevel.GREEN:
        color = (16, 185, 129)
        status = "Ready to Convert"
    elif report.overall_level == ReadinessLevel.YELLOW:
        color = (245, 158, 11)
        status = "Review Recommended"
    else:
        color = (239, 68, 68)
        status = "Action Required"
    
    pdf.set_fill_color(*color)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 24)
    pdf.cell(50, 20, f'{report.overall_score:.0f}%', border=0, ln=0, align='C', fill=True)
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 20, f'  {status}', ln=True)
    pdf.ln(5)
    
    # Score Breakdown
    pdf.set_text_color(15, 23, 42)
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 8, 'Score Breakdown:', ln=True)
    pdf.set_font('Helvetica', '', 10)
    
    scores = [
        ('Data Types', report.datatype_score),
        ('Constraints', report.constraint_score),
        ('Partitions', report.partition_score),
        ('Special Features', report.special_features_score),
    ]
    for name, score in scores:
        pdf.cell(60, 6, f'  {name}:', ln=0)
        pdf.cell(0, 6, f'{score:.0f}%', ln=True)
    pdf.ln(5)
    
    # Summary Statistics
    pdf.set_font('Helvetica', 'B', 14)
    pdf.cell(0, 10, 'Summary Statistics', ln=True)
    pdf.set_font('Helvetica', '', 10)
    
    stats = [
        ('Total Tables', report.tables_total),
        ('Auto-convertible (Green)', report.tables_auto),
        ('Need Review (Yellow)', report.tables_manual),
        ('Blocked (Red)', report.tables_blocked),
        ('Total Columns', report.total_columns),
        ('Total Constraints', report.total_constraints),
    ]
    for name, value in stats:
        pdf.cell(70, 6, f'  {name}:', ln=0)
        pdf.cell(0, 6, str(value), ln=True)
    pdf.ln(5)
    
    # Issues Section - Critical
    if report.critical_issues:
        pdf.set_font('Helvetica', 'B', 14)
        pdf.set_text_color(239, 68, 68)
        pdf.cell(0, 10, f'Critical Issues ({len(report.critical_issues)})', ln=True)
        pdf.set_font('Helvetica', 'I', 9)
        pdf.set_text_color(100, 116, 139)
        pdf.cell(0, 5, 'These must be resolved before migration', ln=True)
        pdf.ln(2)
        
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(15, 23, 42)
        for issue in report.critical_issues:
            # Issue code
            pdf.set_font('Helvetica', 'B', 9)
            pdf.cell(0, 5, f'[{issue.code}]', ln=True)
            # Issue message - use cell with truncation
            pdf.set_font('Helvetica', '', 9)
            msg = issue.message
            if len(msg) > 100:
                msg = msg[:97] + '...'
            pdf.cell(0, 4, f'  {msg}', ln=True)
            
            # Location info
            if issue.table_name:
                location = f'  Location: {issue.table_name}'
                if issue.column_name:
                    location += f' -> {issue.column_name}'
                pdf.set_text_color(100, 116, 139)
                pdf.cell(0, 4, location, ln=True)
                pdf.set_text_color(15, 23, 42)
            
            # Suggestion
            if issue.suggestion:
                pdf.set_text_color(22, 101, 52)
                sug = issue.suggestion
                if len(sug) > 80:
                    sug = sug[:77] + '...'
                pdf.cell(0, 4, f'  Suggestion: {sug}', ln=True)
                pdf.set_text_color(15, 23, 42)
            pdf.ln(2)
        pdf.ln(3)
    
    # Issues Section - Warnings
    if report.warnings:
        pdf.set_font('Helvetica', 'B', 14)
        pdf.set_text_color(245, 158, 11)
        pdf.cell(0, 10, f'Warnings ({len(report.warnings)})', ln=True)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(15, 23, 42)
        
        for issue in report.warnings:
            pdf.set_font('Helvetica', 'B', 9)
            pdf.cell(0, 5, f'[{issue.code}]', ln=True)
            pdf.set_font('Helvetica', '', 9)
            msg = issue.message
            if len(msg) > 100:
                msg = msg[:97] + '...'
            pdf.cell(0, 4, f'  {msg}', ln=True)
            
            if issue.table_name:
                location = f'  Location: {issue.table_name}'
                if issue.column_name:
                    location += f' -> {issue.column_name}'
                pdf.set_text_color(100, 116, 139)
                pdf.cell(0, 4, location, ln=True)
                pdf.set_text_color(15, 23, 42)
            
            if issue.suggestion:
                pdf.set_text_color(22, 101, 52)
                sug = issue.suggestion
                if len(sug) > 80:
                    sug = sug[:77] + '...'
                pdf.cell(0, 4, f'  Suggestion: {sug}', ln=True)
                pdf.set_text_color(15, 23, 42)
            pdf.ln(2)
        pdf.ln(3)
    
    # Issues Section - Info
    if report.info_items:
        pdf.set_font('Helvetica', 'B', 14)
        pdf.set_text_color(99, 102, 241)
        pdf.cell(0, 10, f'Information ({len(report.info_items)})', ln=True)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(15, 23, 42)
        
        for issue in report.info_items:
            text = f'[{issue.code}] {issue.message}'
            if len(text) > 110:
                text = text[:107] + '...'
            pdf.cell(0, 4, text, ln=True)
        pdf.ln(3)
    
    # Per-Table Details
    if report.table_assessments:
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 16)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(0, 10, 'Table-by-Table Analysis', ln=True)
        pdf.ln(3)
        
        for ta in report.table_assessments:
            # Check if we need a new page
            if pdf.get_y() > 250:
                pdf.add_page()
            
            # Table header with color
            if ta.readiness_level == ReadinessLevel.GREEN:
                color = (16, 185, 129)
                status_text = "Auto-convertible"
            elif ta.readiness_level == ReadinessLevel.YELLOW:
                color = (245, 158, 11)
                status_text = "Needs Review"
            else:
                color = (239, 68, 68)
                status_text = "Blocked"
            
            pdf.set_fill_color(*color)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font('Helvetica', 'B', 11)
            pdf.cell(0, 8, f'  {ta.full_name}', ln=True, fill=True)
            
            # Table stats
            pdf.set_text_color(15, 23, 42)
            pdf.set_font('Helvetica', '', 9)
            pdf.cell(0, 5, f'    Score: {ta.readiness_score:.0f}% | Status: {status_text} | Columns: {ta.column_count} | Constraints: {ta.constraint_count}', ln=True)
            
            # Table issues
            if ta.issues:
                pdf.set_font('Helvetica', '', 8)
                pdf.cell(0, 4, f'    Issues ({len(ta.issues)}):', ln=True)
                for issue in ta.issues:
                    text = f'      - [{issue.code}] {issue.message}'
                    if len(text) > 90:
                        text = text[:87] + '...'
                    pdf.cell(0, 3.5, text, ln=True)
                    if issue.suggestion:
                        pdf.set_text_color(22, 101, 52)
                        sug = f'        Suggestion: {issue.suggestion}'
                        if len(sug) > 80:
                            sug = sug[:77] + '...'
                        pdf.cell(0, 3.5, sug, ln=True)
                        pdf.set_text_color(15, 23, 42)
            pdf.ln(4)
    
    return bytes(pdf.output())


def generate_conversion_pdf(result, report) -> bytes:
    """Generate PDF with conversion results - includes full assessment + converted DDL"""
    if not PDF_AVAILABLE:
        return None
    
    pdf = AssessmentPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Conversion Summary
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 10, 'Conversion Summary', ln=True)
    
    if result.success:
        pdf.set_fill_color(16, 185, 129)
        status = "Success"
    else:
        pdf.set_fill_color(239, 68, 68)
        status = "Failed"
    
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(60, 10, f'Status: {status}', border=0, ln=True, fill=True)
    pdf.ln(3)
    
    pdf.set_text_color(15, 23, 42)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 6, f'Tables Converted: {result.tables_converted}', ln=True)
    pdf.cell(0, 6, f'EWI Markers: {result.ewi_count}', ln=True)
    if result.ewi_count > 0:
        pdf.set_text_color(245, 158, 11)
        pdf.set_font('Helvetica', 'I', 9)
        pdf.cell(0, 5, 'Note: Search for "!!!RESOLVE EWI!!!" in the DDL output', ln=True)
        pdf.set_text_color(15, 23, 42)
    pdf.ln(5)
    
    # Assessment Summary Section
    pdf.set_font('Helvetica', 'B', 14)
    pdf.cell(0, 10, 'Assessment Summary', ln=True)
    
    # Score box
    if report.overall_level == ReadinessLevel.GREEN:
        color = (16, 185, 129)
        status_text = "Ready to Convert"
    elif report.overall_level == ReadinessLevel.YELLOW:
        color = (245, 158, 11)
        status_text = "Review Recommended"
    else:
        color = (239, 68, 68)
        status_text = "Action Required"
    
    pdf.set_fill_color(*color)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(40, 12, f'{report.overall_score:.0f}%', border=0, ln=0, align='C', fill=True)
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(0, 12, f'  {status_text}', ln=True)
    pdf.ln(3)
    
    pdf.set_text_color(15, 23, 42)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 6, f'Data Types Score: {report.datatype_score:.0f}%', ln=True)
    pdf.cell(0, 6, f'Constraints Score: {report.constraint_score:.0f}%', ln=True)
    pdf.cell(0, 6, f'Partitions Score: {report.partition_score:.0f}%', ln=True)
    pdf.cell(0, 6, f'Special Features Score: {report.special_features_score:.0f}%', ln=True)
    pdf.ln(3)
    
    # Table Statistics
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 8, 'Table Statistics:', ln=True)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 5, f'  Total Tables: {report.tables_total}', ln=True)
    pdf.cell(0, 5, f'  Auto-convertible: {report.tables_auto}', ln=True)
    pdf.cell(0, 5, f'  Need Review: {report.tables_manual}', ln=True)
    pdf.cell(0, 5, f'  Blocked: {report.tables_blocked}', ln=True)
    pdf.ln(5)
    
    # Issues Summary
    total_issues = len(report.critical_issues) + len(report.warnings) + len(report.info_items)
    if total_issues > 0:
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 8, f'Issues Summary ({total_issues} total):', ln=True)
        pdf.set_font('Helvetica', '', 10)
        if report.critical_issues:
            pdf.set_text_color(239, 68, 68)
            pdf.cell(0, 5, f'  Critical: {len(report.critical_issues)}', ln=True)
        if report.warnings:
            pdf.set_text_color(245, 158, 11)
            pdf.cell(0, 5, f'  Warnings: {len(report.warnings)}', ln=True)
        if report.info_items:
            pdf.set_text_color(99, 102, 241)
            pdf.cell(0, 5, f'  Information: {len(report.info_items)}', ln=True)
        pdf.set_text_color(15, 23, 42)
        pdf.ln(3)
        
        # List critical issues
        if report.critical_issues:
            pdf.set_font('Helvetica', 'B', 11)
            pdf.set_text_color(239, 68, 68)
            pdf.cell(0, 7, 'Critical Issues:', ln=True)
            pdf.set_font('Helvetica', '', 9)
            pdf.set_text_color(15, 23, 42)
            for issue in report.critical_issues:
                # Code
                pdf.set_font('Helvetica', 'B', 9)
                pdf.cell(0, 5, f'[{issue.code}]', ln=True)
                # Message - use cell with truncation for safety
                pdf.set_font('Helvetica', '', 9)
                msg = issue.message
                if len(msg) > 100:
                    msg = msg[:97] + '...'
                pdf.cell(0, 4, f'  {msg}', ln=True)
                # Suggestion
                if issue.suggestion:
                    pdf.set_text_color(22, 101, 52)
                    sug = issue.suggestion
                    if len(sug) > 80:
                        sug = sug[:77] + '...'
                    pdf.cell(0, 4, f'  -> {sug}', ln=True)
                    pdf.set_text_color(15, 23, 42)
            pdf.ln(3)
    
    # Converted DDL - new page
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 10, 'Converted Snowflake Iceberg DDL', ln=True)
    pdf.set_font('Helvetica', 'I', 9)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(0, 5, 'Ready to execute in Snowflake', ln=True)
    pdf.ln(5)
    
    # DDL output - use smaller font and simple cell per line
    pdf.set_font('Courier', '', 6)
    pdf.set_text_color(15, 23, 42)
    
    for line in result.iceberg_ddl.split('\n'):
        # Truncate very long lines
        if len(line) > 130:
            line = line[:127] + '...'
        # Use simple cell, not multi_cell
        pdf.cell(0, 3, line, ln=True)
    
    return bytes(pdf.output())


def create_snowflake_assessment_report(result, ddl: str):
    """Create an assessment report from Snowflake to Iceberg conversion result"""
    from db2ice.assessor import AssessmentReport, Issue, TableAssessment, ReadinessLevel, IssueSeverity
    
    # Parse tables to get count
    parser = SnowflakeParser()
    tables = parser.parse(ddl)
    
    # Create report
    report = AssessmentReport()
    report.tables_total = len(tables)
    
    # Count columns
    total_cols = sum(len(t.columns) for t in tables)
    report.total_columns = total_cols
    
    # Categorize issues from conversion result
    critical = []
    warnings = []
    info = []
    
    for issue in result.issues:
        assessment_issue = Issue(
            code=issue.code,
            severity=IssueSeverity.CRITICAL if issue.severity == 'critical' else 
                     IssueSeverity.WARNING if issue.severity == 'warning' else IssueSeverity.INFO,
            message=issue.message,
            suggestion=issue.suggestion,
            table_name=issue.table_name,
            column_name=issue.column_name
        )
        
        if issue.severity == 'critical':
            critical.append(assessment_issue)
        elif issue.severity == 'warning':
            warnings.append(assessment_issue)
        else:
            info.append(assessment_issue)
    
    report.critical_issues = critical
    report.warnings = warnings
    report.info_items = info
    
    # Calculate scores - Snowflake to Iceberg is generally smoother
    # Base score is high since it's same platform
    base_score = 95
    
    # Deduct for issues
    critical_penalty = len(critical) * 15
    warning_penalty = len(warnings) * 5
    
    report.overall_score = max(0, min(100, base_score - critical_penalty - warning_penalty))
    
    # Set level based on score
    if report.overall_score >= 80:
        report.overall_level = ReadinessLevel.GREEN
    elif report.overall_score >= 50:
        report.overall_level = ReadinessLevel.YELLOW
    else:
        report.overall_level = ReadinessLevel.RED
    
    # Sub-scores (Snowflake types are already compatible)
    report.datatype_score = 98 if not any('type' in i.message.lower() for i in result.issues) else 85
    report.constraint_score = 95  # Constraints handled similarly
    report.partition_score = 100  # N/A for this conversion
    report.special_features_score = 80 if len(warnings) > 0 else 95
    
    # Count table categories - updated for all special table types
    # Blocked: DYNAMIC, EXTERNAL, HYBRID (cannot convert to Iceberg at all)
    # Manual/Warning: TEMPORARY, TRANSIENT (kept as Standard), tables with clustering
    # Auto: Regular tables that convert cleanly
    tables_blocked = sum(1 for t in tables if t.dynamic or t.external or t.hybrid)
    tables_manual = sum(1 for t in tables if t.temporary or t.transient or t.cluster_by)
    report.tables_blocked = tables_blocked
    report.tables_manual = tables_manual
    report.tables_auto = report.tables_total - tables_blocked - tables_manual
    
    # Create per-table assessments
    report.table_assessments = []
    for table in tables:
        ta = TableAssessment(
            table_name=table.name,
            schema=table.schema or "default"
        )
        ta.column_count = len(table.columns)
        ta.constraint_count = (1 if table.primary_key else 0) + len(table.foreign_keys) + len(table.unique_keys)
        
        # Determine readiness based on table type
        if table.dynamic or table.external or table.hybrid:
            # Cannot convert - blocked
            ta.readiness_level = ReadinessLevel.RED
            ta.readiness_score = 0
        elif table.temporary or table.transient:
            # Kept as Standard - needs review
            ta.readiness_level = ReadinessLevel.YELLOW
            ta.readiness_score = 70
        elif table.cluster_by:
            # Minor info - clustering not preserved
            ta.readiness_level = ReadinessLevel.YELLOW
            ta.readiness_score = 85
        else:
            # Clean conversion
            ta.readiness_level = ReadinessLevel.GREEN
            ta.readiness_score = 95
        
        # Add table-specific issues
        ta.issues = [i for i in report.critical_issues + report.warnings + report.info_items 
                    if i.table_name and table.name.upper() in i.table_name.upper()]
        
        report.table_assessments.append(ta)
    
    return report


# Page config
st.set_page_config(
    page_title="DB2ICE",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Premium CSS
st.markdown("""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Global styles */
    .stApp {
        font-family: 'Inter', sans-serif;
    }
    
    /* Hide default header */
    header[data-testid="stHeader"] {
        background: transparent;
    }
    
    /* Main container padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Card styling */
    .premium-card {
        background: white;
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.04);
        border: 1px solid #F1F5F9;
        transition: all 0.2s ease;
    }
    
    .premium-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.08), 0 8px 24px rgba(0,0,0,0.06);
        transform: translateY(-2px);
    }
    
    /* Gradient text */
    .gradient-text {
        background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 50%, #A855F7 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    /* Score ring */
    .score-ring {
        width: 180px;
        height: 180px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 0 auto;
        position: relative;
    }
    
    .score-ring-green {
        background: conic-gradient(#10B981 var(--score), #E2E8F0 0);
    }
    
    .score-ring-yellow {
        background: conic-gradient(#F59E0B var(--score), #E2E8F0 0);
    }
    
    .score-ring-red {
        background: conic-gradient(#EF4444 var(--score), #E2E8F0 0);
    }
    
    .score-inner {
        width: 150px;
        height: 150px;
        border-radius: 50%;
        background: white;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
    }
    
    /* Stat card */
    .stat-card {
        background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        border: 1px solid #E2E8F0;
    }
    
    .stat-value {
        font-size: 2rem;
        font-weight: 700;
        color: #0F172A;
        line-height: 1;
    }
    
    .stat-label {
        font-size: 0.875rem;
        color: #64748B;
        margin-top: 4px;
    }
    
    /* Step indicator */
    .step {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 16px 20px;
        border-radius: 12px;
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        transition: all 0.2s ease;
    }
    
    .step-active {
        background: linear-gradient(135deg, #EEF2FF 0%, #E0E7FF 100%);
        border-color: #6366F1;
    }
    
    .step-complete {
        background: linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%);
        border-color: #10B981;
    }
    
    .step-number {
        width: 32px;
        height: 32px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 600;
        font-size: 0.875rem;
    }
    
    /* Issue cards */
    .issue-critical {
        border-left: 4px solid #EF4444;
        background: linear-gradient(90deg, #FEF2F2 0%, white 100%);
    }
    
    .issue-warning {
        border-left: 4px solid #F59E0B;
        background: linear-gradient(90deg, #FFFBEB 0%, white 100%);
    }
    
    .issue-info {
        border-left: 4px solid #6366F1;
        background: linear-gradient(90deg, #EEF2FF 0%, white 100%);
    }
    
    /* Button overrides */
    .stButton > button {
        border-radius: 10px;
        font-weight: 500;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    .stButton > button:focus {
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.3);
    }
    
    /* Download button styling */
    .stDownloadButton > button {
        border-radius: 10px;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    
    .stDownloadButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    .stDownloadButton > button:active {
        transform: translateY(0);
        background: #4F46E5 !important;
        color: white !important;
    }
    
    .stDownloadButton > button:focus {
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.4);
        outline: none;
    }
    
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Text area styling */
    .stTextArea textarea {
        border-radius: 12px;
        border: 2px solid #E2E8F0;
        font-family: 'SF Mono', 'Fira Code', monospace;
        font-size: 0.875rem;
    }
    
    .stTextArea textarea:focus {
        border-color: #6366F1;
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
    }
    
    /* Code block styling */
    .stCodeBlock {
        border-radius: 12px;
    }
    
    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #6366F1, #8B5CF6);
        border-radius: 999px;
    }
</style>
""", unsafe_allow_html=True)


# Sample DDL
SAMPLE_DDL = """-- ============================================
-- DB2 Sample DDL for DB2ICE Conversion Testing
-- ============================================

-- Customer table with common data types
CREATE TABLE SALES.CUSTOMER (
    CUST_ID INTEGER NOT NULL,
    CUST_NAME VARCHAR(100) NOT NULL,
    EMAIL VARCHAR(255),
    PHONE CHAR(15),
    CREATED_DATE DATE NOT NULL,
    LAST_LOGIN TIMESTAMP,
    STATUS SMALLINT DEFAULT 1,
    PRIMARY KEY (CUST_ID)
);

-- Orders table with XML column (not supported in Iceberg)
CREATE TABLE SALES.ORDERS (
    ORDER_ID INTEGER NOT NULL,
    CUST_ID INTEGER NOT NULL,
    ORDER_DATE DATE NOT NULL,
    TOTAL_AMOUNT DECIMAL(15,2),
    ORDER_XML XML,
    PRIMARY KEY (ORDER_ID),
    FOREIGN KEY (CUST_ID) REFERENCES SALES.CUSTOMER(CUST_ID)
);

-- Sensitive data with FIELDPROC (security feature)
CREATE TABLE SECURE.SENSITIVE_DATA (
    ID INTEGER NOT NULL,
    SSN CHAR(11) FIELDPROC ENCRYPT_SSN,
    PRIMARY KEY (ID)
) EDITPROC SECURE_EDIT;

-- Product catalog with various numeric types
CREATE TABLE INVENTORY.PRODUCTS (
    PRODUCT_ID BIGINT NOT NULL,
    SKU VARCHAR(50) NOT NULL,
    PRODUCT_NAME VARCHAR(200) NOT NULL,
    DESCRIPTION CLOB(1M),
    CATEGORY_CODE SMALLINT,
    UNIT_PRICE DECIMAL(10,2) NOT NULL,
    WEIGHT_KG REAL,
    DIMENSIONS DOUBLE,
    IN_STOCK BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP(6),
    PRIMARY KEY (PRODUCT_ID),
    UNIQUE (SKU)
);

-- Employee records with time types
CREATE TABLE HR.EMPLOYEES (
    EMP_ID INTEGER NOT NULL,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    HIRE_DATE DATE NOT NULL,
    BIRTH_DATE DATE,
    SHIFT_START TIME,
    LAST_REVIEW TIMESTAMP(12),
    SALARY DECIMAL(12,2),
    DEPARTMENT_ID INTEGER,
    MANAGER_ID INTEGER,
    ACTIVE SMALLINT DEFAULT 1,
    PRIMARY KEY (EMP_ID),
    FOREIGN KEY (MANAGER_ID) REFERENCES HR.EMPLOYEES(EMP_ID)
);

-- Financial transactions with high precision
CREATE TABLE FINANCE.TRANSACTIONS (
    TXN_ID BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    ACCOUNT_ID INTEGER NOT NULL,
    TXN_TYPE CHAR(1) NOT NULL,
    AMOUNT DECIMAL(18,4) NOT NULL,
    CURRENCY CHAR(3) DEFAULT 'USD',
    TXN_TIMESTAMP TIMESTAMP NOT NULL,
    POSTED_DATE DATE,
    DESCRIPTION VARCHAR(500),
    REFERENCE_NO VARCHAR(50),
    PRIMARY KEY (TXN_ID)
);

-- Audit log with BLOB for binary data
CREATE TABLE SYSTEM.AUDIT_LOG (
    LOG_ID BIGINT NOT NULL,
    EVENT_TYPE VARCHAR(50) NOT NULL,
    EVENT_TIMESTAMP TIMESTAMP(6) NOT NULL,
    USER_ID VARCHAR(100),
    IP_ADDRESS VARCHAR(45),
    REQUEST_DATA BLOB(100K),
    RESPONSE_CODE INTEGER,
    PRIMARY KEY (LOG_ID)
);

-- Geographic data table
CREATE TABLE GEO.LOCATIONS (
    LOCATION_ID INTEGER NOT NULL,
    LOCATION_NAME VARCHAR(100) NOT NULL,
    LATITUDE DECFLOAT(16),
    LONGITUDE DECFLOAT(16),
    ALTITUDE REAL,
    COUNTRY_CODE CHAR(2),
    POSTAL_CODE VARCHAR(20),
    TIMEZONE VARCHAR(50),
    PRIMARY KEY (LOCATION_ID)
);

-- Table with partitioning (for demo)
CREATE TABLE ANALYTICS.EVENTS (
    EVENT_ID BIGINT NOT NULL,
    EVENT_DATE DATE NOT NULL,
    EVENT_TYPE VARCHAR(100),
    USER_ID INTEGER,
    SESSION_ID VARCHAR(100),
    EVENT_DATA VARCHAR(4000),
    PRIMARY KEY (EVENT_ID, EVENT_DATE)
) PARTITION BY RANGE (EVENT_DATE);

-- ============================================
-- VOLATILE TABLE (Will convert to Snowflake TEMPORARY)
-- Session-scoped - Iceberg doesn't support
-- ============================================

CREATE VOLATILE TABLE SESSION.SHOPPING_CART (
    CART_ID INTEGER NOT NULL,
    SESSION_ID VARCHAR(100) NOT NULL,
    PRODUCT_ID INTEGER,
    QUANTITY INTEGER,
    ADDED_TIMESTAMP TIMESTAMP,
    PRIMARY KEY (CART_ID)
);

-- ============================================
-- GLOBAL TEMPORARY TABLE (Will convert to TEMPORARY)
-- Session-scoped - Iceberg doesn't support
-- ============================================

CREATE GLOBAL TEMPORARY TABLE SESSION.TEMP_CALC_RESULTS (
    CALC_ID INTEGER NOT NULL,
    INPUT_VALUE DECIMAL(18,4),
    RESULT_VALUE DECIMAL(18,4),
    CALC_TIMESTAMP TIMESTAMP,
    PRIMARY KEY (CALC_ID)
);

-- DECLARE style temporary table (alternative syntax)
DECLARE GLOBAL TEMPORARY TABLE SESSION.WORK_DATA (
    WORK_ID INTEGER NOT NULL,
    DATA_KEY VARCHAR(50),
    DATA_VALUE VARCHAR(500),
    PRIMARY KEY (WORK_ID)
);"""


# Sample Snowflake Standard DDL
SAMPLE_SNOWFLAKE_DDL = """-- ============================================
-- Snowflake Standard DDL for Iceberg Conversion
-- Demonstrates: Regular, TEMPORARY, TRANSIENT, 
--               DYNAMIC, EXTERNAL, and HYBRID tables
-- ============================================

-- ============================================
-- REGULAR TABLES (Will convert to Iceberg)
-- ============================================

-- Customer dimension table with clustering
CREATE OR REPLACE TABLE ANALYTICS.DIM_CUSTOMER (
    CUSTOMER_ID NUMBER(38,0) NOT NULL AUTOINCREMENT,
    CUSTOMER_KEY VARCHAR(50) NOT NULL,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    PHONE VARCHAR(20),
    ADDRESS VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (CUSTOMER_ID),
    UNIQUE (CUSTOMER_KEY)
)
CLUSTER BY (CUSTOMER_KEY)
DATA_RETENTION_TIME_IN_DAYS = 90
CHANGE_TRACKING = TRUE
COMMENT = 'Customer dimension table';

-- Fact table with masking policy
CREATE OR REPLACE TABLE ANALYTICS.FACT_SALES (
    SALE_ID NUMBER(38,0) NOT NULL,
    CUSTOMER_ID NUMBER(38,0) NOT NULL,
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    SALE_DATE DATE NOT NULL,
    QUANTITY NUMBER(10,0),
    UNIT_PRICE NUMBER(18,4),
    TOTAL_AMOUNT NUMBER(18,4),
    DISCOUNT_PCT NUMBER(5,2),
    TAX_AMOUNT NUMBER(18,4),
    PAYMENT_METHOD VARCHAR(50),
    SALES_REP_ID NUMBER(38,0),
    REGION VARCHAR(50),
    PRIMARY KEY (SALE_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES ANALYTICS.DIM_CUSTOMER(CUSTOMER_ID)
)
CLUSTER BY (SALE_DATE, REGION);

-- Product table with geography type
CREATE OR REPLACE TABLE CATALOG.PRODUCTS (
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    SKU VARCHAR(50) NOT NULL,
    PRODUCT_NAME VARCHAR(200) NOT NULL,
    CATEGORY VARCHAR(100),
    SUBCATEGORY VARCHAR(100),
    DESCRIPTION VARCHAR(4000),
    UNIT_COST NUMBER(18,4),
    LIST_PRICE NUMBER(18,4),
    WEIGHT_KG FLOAT,
    DIMENSIONS OBJECT,
    WAREHOUSE_LOCATION GEOGRAPHY,
    TAGS ARRAY,
    METADATA VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PRODUCT_ID),
    UNIQUE (SKU)
)
COMMENT = 'Product catalog with spatial data';

-- Time dimension
CREATE OR REPLACE TABLE ANALYTICS.DIM_DATE (
    DATE_KEY NUMBER(8,0) NOT NULL,
    FULL_DATE DATE NOT NULL,
    YEAR NUMBER(4,0),
    QUARTER NUMBER(1,0),
    MONTH NUMBER(2,0),
    MONTH_NAME VARCHAR(20),
    WEEK_OF_YEAR NUMBER(2,0),
    DAY_OF_MONTH NUMBER(2,0),
    DAY_OF_WEEK NUMBER(1,0),
    DAY_NAME VARCHAR(20),
    IS_WEEKEND BOOLEAN,
    IS_HOLIDAY BOOLEAN,
    FISCAL_YEAR NUMBER(4,0),
    FISCAL_QUARTER NUMBER(1,0),
    PRIMARY KEY (DATE_KEY)
);

-- ============================================
-- TEMPORARY TABLE (Will keep as Standard)
-- Session-scoped, cannot convert to Iceberg
-- ============================================

CREATE TEMPORARY TABLE STAGING.SESSION_CART (
    CART_ID NUMBER(38,0) NOT NULL,
    SESSION_ID VARCHAR(100) NOT NULL,
    PRODUCT_ID NUMBER(38,0),
    QUANTITY NUMBER(10,0),
    ADDED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CART_ID)
);

-- ============================================
-- TRANSIENT TABLE (Will keep as Standard)
-- No Fail-safe, preserves original behavior
-- ============================================

CREATE TRANSIENT TABLE STAGING.STG_ORDERS (
    ORDER_ID NUMBER(38,0) NOT NULL,
    CUSTOMER_ID NUMBER(38,0),
    ORDER_DATE DATE,
    TOTAL_AMOUNT NUMBER(18,2),
    ORDER_STATUS VARCHAR(20),
    RAW_DATA VARIANT,
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- DYNAMIC TABLE (Will be skipped)
-- Auto-refresh from query - incompatible
-- ============================================

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DAILY_SALES_SUMMARY (
    SALE_DATE DATE,
    TOTAL_SALES NUMBER(18,4),
    ORDER_COUNT NUMBER(38,0),
    AVG_ORDER_VALUE NUMBER(18,4)
)
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT 
    SALE_DATE,
    SUM(TOTAL_AMOUNT) AS TOTAL_SALES,
    COUNT(*) AS ORDER_COUNT,
    AVG(TOTAL_AMOUNT) AS AVG_ORDER_VALUE
FROM ANALYTICS.FACT_SALES
GROUP BY SALE_DATE;

-- ============================================
-- EXTERNAL TABLE (Will be skipped)
-- References external stage data
-- ============================================

CREATE OR REPLACE EXTERNAL TABLE RAW.EXT_CUSTOMER_EVENTS (
    EVENT_ID VARCHAR AS (VALUE:event_id::VARCHAR),
    EVENT_TYPE VARCHAR AS (VALUE:event_type::VARCHAR),
    CUSTOMER_ID NUMBER AS (VALUE:customer_id::NUMBER),
    EVENT_TIMESTAMP TIMESTAMP_NTZ AS (VALUE:timestamp::TIMESTAMP_NTZ),
    EVENT_DATA VARIANT AS (VALUE:data)
)
WITH LOCATION = @RAW.EVENTS_STAGE/
AUTO_REFRESH = TRUE
FILE_FORMAT = (TYPE = JSON);

-- ============================================
-- HYBRID TABLE (Will be skipped)  
-- HTAP-optimized - different performance model
-- ============================================

CREATE OR REPLACE HYBRID TABLE OPERATIONAL.INVENTORY_LEVELS (
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    WAREHOUSE_ID NUMBER(38,0) NOT NULL,
    QUANTITY_ON_HAND NUMBER(10,0) NOT NULL,
    QUANTITY_RESERVED NUMBER(10,0) DEFAULT 0,
    QUANTITY_AVAILABLE NUMBER(10,0),
    LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PRODUCT_ID, WAREHOUSE_ID)
);"""


def render_header():
    """Render the premium header with explanatory text"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 1rem 0 1.5rem 0;">
            <div style="font-size: 3.5rem; margin-bottom: 0.5rem;">❄️</div>
            <h1 style="
                font-size: 2.5rem;
                font-weight: 700;
                margin: 0;
                letter-spacing: -0.02em;
            "><span class="gradient-text">DB2ICE</span></h1>
            <p style="
                color: #64748B;
                font-size: 1.125rem;
                margin: 0.5rem 0 0 0;
            ">Convert to Snowflake Managed Iceberg Tables</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Add explanatory section
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #EEF2FF 0%, #E0E7FF 100%);
        border-radius: 12px;
        padding: 1.25rem 1.5rem;
        margin: 0 auto 1.5rem auto;
        max-width: 900px;
        border: 1px solid #C7D2FE;
    ">
        <div style="display: flex; align-items: flex-start; gap: 1rem;">
            <div style="font-size: 1.5rem;">💡</div>
            <div>
                <div style="font-weight: 600; color: #3730A3; margin-bottom: 0.5rem; font-size: 1rem;">
                    What is DB2ICE?
                </div>
                <div style="color: #4338CA; font-size: 0.9rem; line-height: 1.6;">
                    This tool converts table definitions into Snowflake Managed Iceberg tables. 
                    <strong>Supported sources:</strong> IBM DB2 DDL or existing Snowflake Standard tables.
                    It analyzes your DDL for migration readiness, maps data types to Iceberg-compatible types, 
                    and flags potential issues using industry-standard EWI (Error/Warning/Issue) markers.
                </div>
                <div style="
                    display: flex;
                    gap: 1.5rem;
                    margin-top: 0.75rem;
                    flex-wrap: wrap;
                ">
                    <div style="display: flex; align-items: center; gap: 0.4rem; font-size: 0.85rem; color: #4338CA;">
                        <span style="font-weight: 600;">1.</span> Select source type in Configuration
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.4rem; font-size: 0.85rem; color: #4338CA;">
                        <span style="font-weight: 600;">2.</span> Paste your DDL or try a sample
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.4rem; font-size: 0.85rem; color: #4338CA;">
                        <span style="font-weight: 600;">3.</span> Click "Assess & Convert"
                    </div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_steps(current_step: int):
    """Render the step indicator"""
    steps = [
        ("1", "Input DDL", "Paste or upload your DDL"),
        ("2", "Assess", "Analyze migration readiness"),
        ("3", "Convert", "Generate Iceberg DDL"),
    ]
    
    cols = st.columns(3, gap="medium")
    
    for i, (col, (num, title, desc)) in enumerate(zip(cols, steps)):
        with col:
            if i < current_step:
                state = "complete"
                icon = "✓"
                bg = "#10B981"
                text_color = "white"
            elif i == current_step:
                state = "active"
                icon = num
                bg = "#6366F1"
                text_color = "white"
            else:
                state = "pending"
                icon = num
                bg = "#E2E8F0"
                text_color = "#64748B"
            
            st.markdown(f"""
            <div class="step step-{state}" style="cursor: default;">
                <div class="step-number" style="background: {bg}; color: {text_color};">
                    {icon}
                </div>
                <div>
                    <div style="font-weight: 600; color: #0F172A; font-size: 0.9375rem;">{title}</div>
                    <div style="color: #64748B; font-size: 0.8125rem;">{desc}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)


def render_input_card():
    """Render the DDL input card with source type selection"""
    
    # Initialize source_type in session state if not present
    if 'source_type' not in st.session_state:
        st.session_state.source_type = 'db2'
    
    st.markdown("""
    <div style="margin: 2rem 0 1rem 0;">
        <h3 style="font-size: 1.25rem; font-weight: 600; color: #0F172A; margin-bottom: 0.5rem;">
            Source DDL
        </h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Source Type Selection - Prominent tabs above input
    col_source, col_spacer = st.columns([3, 1])
    
    with col_source:
        source_options = ["IBM DB2", "Snowflake Standard"]
        current_index = 0 if st.session_state.source_type == 'db2' else 1
        
        # Use segmented control style with selectbox for reliability
        selected = st.radio(
            "Select source DDL type:",
            options=source_options,
            index=current_index,
            horizontal=True,
            key="source_type_radio",
            help="Choose the format of your input DDL"
        )
        
        # Update session state
        new_source_type = 'db2' if selected == "IBM DB2" else 'snowflake'
        if new_source_type != st.session_state.source_type:
            st.session_state.source_type = new_source_type
            # Clear input when switching source types to avoid confusion
            if 'input_ddl' in st.session_state:
                st.session_state.input_ddl = ''
            st.rerun()
    
    # Show description based on selection
    if st.session_state.source_type == 'db2':
        st.info("📥 **DB2 → Iceberg**: Convert IBM DB2 DDL to Snowflake Managed Iceberg tables", icon="🔄")
    else:
        st.info("📥 **Snowflake Standard → Iceberg**: Convert existing Snowflake tables to Managed Iceberg format", icon="🔄")
    
    # Input area
    col_main, col_side = st.columns([4, 1], gap="large")
    
    with col_main:
        # Dynamic placeholder based on source type
        if st.session_state.source_type == 'db2':
            placeholder = "-- DB2 DDL\nCREATE TABLE SCHEMA.TABLE_NAME (\n    COLUMN1 INTEGER NOT NULL,\n    COLUMN2 VARCHAR(100),\n    PRIMARY KEY (COLUMN1)\n);"
        else:
            placeholder = "-- Snowflake DDL\nCREATE TABLE SCHEMA.TABLE_NAME (\n    COLUMN1 NUMBER(38,0) NOT NULL,\n    COLUMN2 VARCHAR(100),\n    PRIMARY KEY (COLUMN1)\n);"
        
        ddl = st.text_area(
            "DDL Input",
            value=st.session_state.get('input_ddl', ''),
            height=300,
            placeholder=placeholder,
            label_visibility="collapsed"
        )
        # Always sync back to session state
        st.session_state.input_ddl = ddl
    
    with col_side:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%);
            border-radius: 12px;
            padding: 16px;
            border: 1px dashed #CBD5E1;
            text-align: center;
            margin-bottom: 0.75rem;
        ">
            <div style="font-size: 1.5rem; margin-bottom: 0.25rem;">📄</div>
            <div style="font-size: 0.8rem; color: #64748B;">Upload file</div>
        </div>
        """, unsafe_allow_html=True)
        
        uploaded = st.file_uploader("Upload", type=['sql', 'ddl', 'txt'], label_visibility="collapsed")
        if uploaded:
            content = uploaded.read().decode('utf-8')
            st.session_state.input_ddl = content
            st.rerun()
        
        st.markdown("<div style='height: 0.5rem'></div>", unsafe_allow_html=True)
        
        # Sample button with clear label showing which sample will load
        sample_label = "✨ DB2 Sample" if st.session_state.source_type == 'db2' else "✨ Snowflake Sample"
        if st.button(sample_label, use_container_width=True, help=f"Load sample {st.session_state.source_type.upper()} DDL"):
            if st.session_state.source_type == 'snowflake':
                st.session_state.input_ddl = SAMPLE_SNOWFLAKE_DDL
            else:
                st.session_state.input_ddl = SAMPLE_DDL
            st.rerun()
    
    return st.session_state.get('input_ddl', '')


def render_config_panel():
    """Render inline configuration for Iceberg output settings"""
    with st.expander("⚙️ Iceberg Configuration", expanded=False):
        # Iceberg output settings
        st.markdown("##### Output Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            st.session_state.external_volume = st.text_input(
                "External volume",
                value=st.session_state.get('external_volume', 'my_iceberg_volume'),
                help="Snowflake external volume name for Iceberg storage"
            )
        
        with col2:
            st.session_state.base_location = st.text_input(
                "Base location pattern",
                value=st.session_state.get('base_location', '{schema}/{table}'),
                help="Storage path pattern (supports {schema}, {table})"
            )
        
        st.divider()
        
        # Output options
        st.markdown("##### Output Options")
        col3, col4 = st.columns(2)
        
        with col3:
            st.session_state.include_ewi = st.toggle(
                "Include EWI markers",
                value=st.session_state.get('include_ewi', True),
                help="Add !!!RESOLVE EWI!!! markers for issues requiring manual review"
            )
        
        with col4:
            st.session_state.include_comments = st.toggle(
                "Include comments",
                value=st.session_state.get('include_comments', True),
                help="Add SQL comments explaining conversions and constraints"
            )


def render_action_buttons():
    """Render action buttons - highlight the one that was used"""
    
    # Check what results exist to show which button was used
    has_report = 'last_report' in st.session_state
    has_conversion = 'last_result' in st.session_state
    
    col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
    
    with col1:
        # Use secondary style, highlight if assess-only was done
        btn_type = "primary" if (has_report and not has_conversion) else "secondary"
        assess = st.button(
            "📊  Assess Readiness",
            type=btn_type,
            use_container_width=True
        )
    
    with col2:
        # Use primary style if conversion was done
        btn_type = "primary" if has_conversion else "secondary"
        convert = st.button(
            "✨  Assess & Convert",
            type=btn_type,
            use_container_width=True
        )
    
    with col3:
        clear = st.button("Clear", use_container_width=True)
        if clear:
            for key in ['input_ddl', 'last_report', 'last_result']:
                st.session_state.pop(key, None)
            st.rerun()
    
    with col4:
        docs = st.button("📖 Docs", use_container_width=True, help="View conversion documentation")
        if docs:
            st.session_state.show_docs = True
            st.rerun()
    
    return assess, convert


def render_documentation():
    """Render the documentation modal/section"""
    if not st.session_state.get('show_docs', False):
        return
    
    with st.container():
        col1, col2 = st.columns([6, 1])
        with col1:
            st.markdown("## 📖 DB2ICE Conversion Documentation")
        with col2:
            if st.button("✕ Close", key="close_docs"):
                st.session_state.show_docs = False
                st.rerun()
        
        st.divider()
        
        # Create tabs for different documentation sections
        tab1, tab2, tab3, tab4 = st.tabs(["🔄 Table Behavior", "📊 Data Type Mapping", "⚠️ EWI Codes", "✨ Features"])
        
        with tab1:
            st.markdown("""
            ### Table Type Handling
            
            DB2ICE handles different table types appropriately based on their characteristics:
            
            #### IBM DB2 Source Tables
            
            | DB2 Table Type | Converted To | Reason |
            |----------------|--------------|--------|
            | **Regular TABLE** | Iceberg Table | Full conversion with type mapping |
            | **VOLATILE TABLE** | Snowflake TEMPORARY | Session-scoped, Iceberg doesn't support |
            | **GLOBAL TEMPORARY** | Snowflake TEMPORARY | Session-scoped, Iceberg doesn't support |
            | **DECLARE GLOBAL TEMPORARY** | Snowflake TEMPORARY | Session-scoped, Iceberg doesn't support |
            
            #### Snowflake Standard Source Tables
            
            | Snowflake Table Type | Converted To | Reason |
            |----------------------|--------------|--------|
            | **Regular TABLE** | Iceberg Table | Full conversion |
            | **TEMPORARY TABLE** | Kept as TEMPORARY | Session-scoped, Iceberg doesn't support |
            | **TRANSIENT TABLE** | Kept as TRANSIENT | No Fail-safe needed, preserve behavior |
            | **DYNAMIC TABLE** | ❌ Skipped | Auto-refresh from query - incompatible |
            | **EXTERNAL TABLE** | ❌ Skipped | Already on external storage |
            | **HYBRID TABLE** | ❌ Skipped | HTAP-optimized, different model |
            
            ---
            
            ### Iceberg Table Output Format
            
            ```sql
            CREATE OR REPLACE ICEBERG TABLE schema.table_name (
                column1 data_type NOT NULL,
                column2 data_type,
                PRIMARY KEY (column1)
            )
            CATALOG = 'SNOWFLAKE'
            EXTERNAL_VOLUME = 'your_volume'
            BASE_LOCATION = 'schema/table_name';
            ```
            
            **Required Iceberg Clauses:**
            - `CATALOG = 'SNOWFLAKE'` - Uses Snowflake as the Iceberg catalog
            - `EXTERNAL_VOLUME` - Specifies the storage location
            - `BASE_LOCATION` - Path within the external volume
            """)
        
        with tab2:
            col_db2, col_sf = st.columns(2)
            
            with col_db2:
                st.markdown("""
                ### DB2 → Iceberg Type Mapping
                
                | DB2 Type | Iceberg Type | Notes |
                |----------|--------------|-------|
                | `SMALLINT` | `SMALLINT` | Direct mapping |
                | `INTEGER/INT` | `INTEGER` | Direct mapping |
                | `BIGINT` | `BIGINT` | Direct mapping |
                | `DECIMAL(p,s)` | `NUMBER(p,s)` | Precision preserved |
                | `REAL` | `FLOAT` | Single precision |
                | `DOUBLE` | `DOUBLE` | Double precision |
                | `DECFLOAT` | `DOUBLE` | ⚠️ Precision loss possible |
                | `CHAR(n)` | `VARCHAR(n)` | Converted to variable |
                | `VARCHAR(n)` | `VARCHAR(n)` | Direct mapping |
                | `CLOB` | `VARCHAR(16777216)` | ⚠️ Size limits |
                | `BLOB` | `BINARY` | Binary data |
                | `DATE` | `DATE` | Direct mapping |
                | `TIME` | `TIME(6)` | ⚠️ Precision forced to 6 |
                | `TIMESTAMP(p)` | `TIMESTAMP_NTZ(6)` | ⚠️ Precision forced to 6 |
                | `BOOLEAN` | `BOOLEAN` | Direct mapping |
                | `XML` | ❌ Blocked | Not supported |
                | `ROWID` | ❌ Blocked | System-generated |
                """)
            
            with col_sf:
                st.markdown("""
                ### Snowflake → Iceberg Type Mapping
                
                #### ✅ Supported Types (Direct Mapping)
                
                | Snowflake Type | Iceberg Type | Notes |
                |----------------|--------------|-------|
                | `NUMBER(p,s)` | `NUMBER(p,s)` | Preserved |
                | `INT/INTEGER` | `NUMBER(10,0)` | 32-bit signed integer |
                | `BIGINT` | `NUMBER(19,0)` | 64-bit signed integer |
                | `FLOAT/DOUBLE` | `FLOAT` | 64-bit floating point |
                | `VARCHAR(n)` | `VARCHAR(n)` | Max 128MB for Iceberg |
                | `STRING/TEXT` | `VARCHAR` | Max 128MB |
                | `BINARY` | `BINARY` | Preserved |
                | `BOOLEAN` | `BOOLEAN` | Preserved |
                | `DATE` | `DATE` | Preserved |
                | `TIME` | `TIME(6)` | ⚠️ Forced to 6 (μs) |
                | `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ(6)` | ⚠️ Forced to 6 (μs) |
                | `TIMESTAMP_LTZ` | `TIMESTAMP_LTZ(6)` | ⚠️ Forced to 6 (μs) |
                
                #### ❌ NOT Supported in Iceberg (Converted to VARCHAR)
                
                | Snowflake Type | Converted To | Warning |
                |----------------|--------------|---------|
                | `VARIANT` | `VARCHAR` | 🔴 Semi-structured not supported |
                | `OBJECT` | `VARCHAR` | 🔴 Use structured OBJECT instead |
                | `ARRAY` | `VARCHAR` | 🔴 Use structured ARRAY instead |
                | `GEOGRAPHY` | `VARCHAR` | 🔴 Store as WKT/GeoJSON |
                | `GEOMETRY` | `VARCHAR` | 🔴 Store as WKT/GeoJSON |
                | `TIMESTAMP_TZ` | `TIMESTAMP_LTZ(6)` | ⚠️ TZ handling changes |
                
                ---
                
                **Important Notes:**
                - Iceberg requires TIME/TIMESTAMP precision of 6 (microseconds)
                - Semi-structured types (VARIANT, OBJECT, ARRAY) must be serialized to VARCHAR
                - Use **structured** OBJECT/ARRAY types with defined schemas for Iceberg
                """)
        
        with tab3:
            st.markdown("""
            ### EWI (Error/Warning/Issue) Codes
            
            DB2ICE uses SnowConvert-style EWI markers to flag issues requiring attention:
            
            ```sql
            !!!RESOLVE EWI!!! /*** SSC-EWI-xxx - message ***/!!!
            ```
            
            #### DB2 Source EWI Codes
            
            | Code | Severity | Description |
            |------|----------|-------------|
            | `SSC-EWI-DB2ICE-0001` | 🔴 Critical | XML data type not supported |
            | `SSC-EWI-DB2ICE-0002` | 🔴 Critical | EDITPROC detected - data transformation |
            | `SSC-EWI-DB2ICE-0003` | 🔴 Critical | VALIDPROC detected - validation logic |
            | `SSC-EWI-DB2ICE-0004` | 🔴 Critical | FIELDPROC detected - encryption |
            | `SSC-EWI-DB2ICE-0010` | 🟡 Warning | GENERATED ALWAYS AS IDENTITY not supported |
            | `SSC-EWI-DB2ICE-0011` | 🟡 Warning | TIMESTAMP precision reduced to 6 |
            | `SSC-EWI-DB2ICE-0012` | 🟡 Warning | DECFLOAT converted with potential precision loss |
            | `SSC-EWI-DB2ICE-0030` | 🟢 Info | VOLATILE/GLOBAL TEMPORARY kept as TEMPORARY |
            
            #### Snowflake Source EWI Codes
            
            | Code | Severity | Description |
            |------|----------|-------------|
            | `SSC-EWI-SF2ICE-0001` | 🔴 Critical | VARIANT not supported - converted to VARCHAR |
            | `SSC-EWI-SF2ICE-0002` | 🔴 Critical | Semi-structured OBJECT not supported - use structured type |
            | `SSC-EWI-SF2ICE-0003` | 🔴 Critical | Semi-structured ARRAY not supported - use structured type |
            | `SSC-EWI-SF2ICE-0004` | 🔴 Critical | GEOGRAPHY type converted to VARCHAR |
            | `SSC-EWI-SF2ICE-0005` | 🔴 Critical | GEOMETRY type converted to VARCHAR |
            | `SSC-EWI-SF2ICE-0006` | 🟢 Info | TIME precision adjusted to 6 |
            | `SSC-EWI-SF2ICE-0007` | 🟢 Info | TIMESTAMP precision adjusted to 6 |
            | `SSC-EWI-SF2ICE-0008` | 🟢 Info | TIMESTAMP_LTZ precision adjusted to 6 |
            | `SSC-EWI-SF2ICE-0009` | 🟡 Warning | TIMESTAMP_TZ converted to TIMESTAMP_LTZ(6) |
            | `SSC-EWI-SF2ICE-0015` | 🟡 Warning | IDENTITY/AUTOINCREMENT not supported |
            | `SSC-EWI-SF2ICE-0016` | 🟡 Warning | MASKING POLICY needs re-application |
            | `SSC-EWI-SF2ICE-0017` | 🟢 Info | COLLATE clause not supported |
            | `SSC-EWI-SF2ICE-0020` | 🟢 Info | TEMPORARY table kept as Standard |
            | `SSC-EWI-SF2ICE-0021` | 🟢 Info | TRANSIENT table kept as Standard |
            | `SSC-EWI-SF2ICE-0022` | 🔴 Critical | DYNAMIC table cannot be converted |
            | `SSC-EWI-SF2ICE-0023` | 🔴 Critical | EXTERNAL table cannot be converted |
            | `SSC-EWI-SF2ICE-0024` | 🔴 Critical | HYBRID table cannot be converted |
            """)
        
        with tab4:
            st.markdown("""
            ### Implemented Features
            
            #### Core Conversion
            - ✅ Full DDL parsing for DB2 and Snowflake Standard
            - ✅ Data type mapping with automatic conversion
            - ✅ Primary key constraint preservation
            - ✅ Foreign key and unique constraint documentation
            - ✅ NOT NULL constraint preservation
            - ✅ Iceberg-specific clause generation (CATALOG, EXTERNAL_VOLUME, BASE_LOCATION)
            
            #### Special Table Handling
            - ✅ DB2 VOLATILE tables → Snowflake TEMPORARY
            - ✅ DB2 GLOBAL TEMPORARY tables → Snowflake TEMPORARY
            - ✅ Snowflake TEMPORARY tables → Kept as Standard
            - ✅ Snowflake TRANSIENT tables → Kept as Standard
            - ✅ Snowflake DYNAMIC/EXTERNAL/HYBRID tables → Skipped with error
            
            #### Assessment & Reporting
            - ✅ Readiness scoring (Green/Yellow/Red)
            - ✅ Per-table assessment with issues
            - ✅ Data type distribution analysis
            - ✅ Critical issue identification
            - ✅ PDF report generation
            - ✅ DDL download
            
            #### EWI Markers
            - ✅ SnowConvert-style EWI format
            - ✅ Critical issues for blocking problems
            - ✅ Warnings for items needing review
            - ✅ Info items for documentation
            
            #### DB2-Specific Features
            - ✅ EDITPROC/VALIDPROC/FIELDPROC detection
            - ✅ XML type blocking
            - ✅ TIMESTAMP precision handling
            - ✅ CHAR to VARCHAR conversion
            - ✅ CLOB/BLOB size handling
            - ✅ FOR BIT DATA support
            - ✅ CCSID handling
            - ✅ Partition documentation
            
            #### Snowflake-Specific Features
            - ✅ CLUSTER BY documentation
            - ✅ GEOGRAPHY/GEOMETRY type → VARCHAR conversion
            - ✅ VARIANT/OBJECT/ARRAY → VARCHAR (not supported in Iceberg)
            - ✅ TIME/TIMESTAMP precision adjustment to 6 (microseconds)
            - ✅ IDENTITY column detection
            - ✅ Masking policy detection
            - ✅ Data retention and change tracking notes
            """)
        
        st.divider()
        st.caption("DB2ICE - Convert to Snowflake Managed Iceberg Tables")


def render_score_visualization(report):
    """Render the premium score visualization using native Streamlit components"""
    score = report.overall_score
    
    if report.overall_level == ReadinessLevel.GREEN:
        color = "#10B981"
        bg = "#ECFDF5"
        status = "Ready to convert"
    elif report.overall_level == ReadinessLevel.YELLOW:
        color = "#F59E0B"
        bg = "#FFFBEB"
        status = "Review recommended"
    else:
        color = "#EF4444"
        bg = "#FEF2F2"
        status = "Action required"
    
    # Header only (download button moved to separate section)
    st.markdown("""
    <h3 style="font-size: 1.25rem; font-weight: 600; color: #0F172A; margin: 1.5rem 0 0.5rem 0;">
        📊 Assessment Results
    </h3>
    """, unsafe_allow_html=True)
    
    # Score card using columns
    st.markdown(f"""
    <div style="
        background: {bg};
        border-radius: 16px;
        padding: 2rem;
        margin: 0.5rem 0;
        border: 1px solid {color}33;
    ">
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # Score circle
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem;">
            <div style="
                width: 140px;
                height: 140px;
                border-radius: 50%;
                background: conic-gradient({color} {score}%, #E2E8F0 0);
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto;
            ">
                <div style="
                    width: 110px;
                    height: 110px;
                    border-radius: 50%;
                    background: white;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                ">
                    <div style="font-size: 2.5rem; font-weight: 700; color: {color}; line-height: 1;">{score:.0f}</div>
                    <div style="font-size: 0.875rem; color: #64748B;">percent</div>
                </div>
            </div>
            <div style="margin-top: 1rem;">
                <span style="
                    display: inline-block;
                    padding: 0.5rem 1rem;
                    background: {color};
                    color: white;
                    border-radius: 999px;
                    font-weight: 600;
                    font-size: 0.875rem;
                ">{status}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <h4 style="margin: 0 0 1rem 0; color: #0F172A; font-weight: 600;">Score Breakdown</h4>
        """, unsafe_allow_html=True)
        
        # Data Types progress
        st.markdown(f"""
        <div style="margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="font-size: 0.875rem; color: #374151;">Data Types</span>
                <span style="font-size: 0.875rem; font-weight: 600; color: #0F172A;">{report.datatype_score:.0f}%</span>
            </div>
            <div style="height: 8px; background: #E2E8F0; border-radius: 999px; overflow: hidden;">
                <div style="height: 100%; width: {report.datatype_score}%; background: linear-gradient(90deg, #6366F1, #8B5CF6); border-radius: 999px;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Constraints progress
        st.markdown(f"""
        <div style="margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="font-size: 0.875rem; color: #374151;">Constraints</span>
                <span style="font-size: 0.875rem; font-weight: 600; color: #0F172A;">{report.constraint_score:.0f}%</span>
            </div>
            <div style="height: 8px; background: #E2E8F0; border-radius: 999px; overflow: hidden;">
                <div style="height: 100%; width: {report.constraint_score}%; background: linear-gradient(90deg, #6366F1, #8B5CF6); border-radius: 999px;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Partitions progress
        st.markdown(f"""
        <div style="margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="font-size: 0.875rem; color: #374151;">Partitions</span>
                <span style="font-size: 0.875rem; font-weight: 600; color: #0F172A;">{report.partition_score:.0f}%</span>
            </div>
            <div style="height: 8px; background: #E2E8F0; border-radius: 999px; overflow: hidden;">
                <div style="height: 100%; width: {report.partition_score}%; background: linear-gradient(90deg, #6366F1, #8B5CF6); border-radius: 999px;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Special Features progress
        st.markdown(f"""
        <div>
            <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="font-size: 0.875rem; color: #374151;">Special Features</span>
                <span style="font-size: 0.875rem; font-weight: 600; color: #0F172A;">{report.special_features_score:.0f}%</span>
            </div>
            <div style="height: 8px; background: #E2E8F0; border-radius: 999px; overflow: hidden;">
                <div style="height: 100%; width: {report.special_features_score}%; background: linear-gradient(90deg, #6366F1, #8B5CF6); border-radius: 999px;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Close the container div
    st.markdown("</div>", unsafe_allow_html=True)


def render_stats(report):
    """Render stats cards"""
    st.markdown("""
    <h3 style="font-size: 1.125rem; font-weight: 600; color: #0F172A; margin: 1.5rem 0 1rem 0;">
        Summary
    </h3>
    """, unsafe_allow_html=True)
    
    cols = st.columns(5, gap="small")
    
    stats = [
        (report.tables_total, "Tables", "#6366F1", "📋"),
        (report.tables_auto, "Auto-convert", "#10B981", "✅"),
        (report.tables_manual, "Review", "#F59E0B", "⚠️"),
        (report.tables_blocked, "Blocked", "#EF4444", "🚫"),
        (report.total_columns, "Columns", "#8B5CF6", "📊"),
    ]
    
    for col, (value, label, color, icon) in zip(cols, stats):
        with col:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 1.5rem; margin-bottom: 0.25rem;">{icon}</div>
                <div class="stat-value" style="color: {color};">{value}</div>
                <div class="stat-label">{label}</div>
            </div>
            """, unsafe_allow_html=True)


def render_issues(report):
    """Render issues section"""
    
    if report.critical_issues:
        st.markdown(f"""
        <h3 style="font-size: 1.125rem; font-weight: 600; color: #EF4444; margin: 2rem 0 1rem 0; display: flex; align-items: center; gap: 0.5rem;">
            <span>🚨</span> Critical Issues ({len(report.critical_issues)})
        </h3>
        <p style="color: #64748B; font-size: 0.875rem; margin: 0 0 1rem 0;">
            These must be resolved before migration
        </p>
        """, unsafe_allow_html=True)
        
        for issue in report.critical_issues:
            st.markdown(f"""
            <div class="premium-card issue-critical" style="margin-bottom: 1rem; padding: 1.25rem;">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
                    <code style="background: #FEE2E2; color: #991B1B; padding: 0.25rem 0.75rem; border-radius: 6px; font-size: 0.8125rem; font-weight: 600;">{issue.code}</code>
                </div>
                <p style="color: #0F172A; font-size: 0.9375rem; margin: 0 0 0.5rem 0; font-weight: 500;">{issue.message}</p>
                {"<p style='color: #64748B; font-size: 0.8125rem; margin: 0;'>📍 " + issue.table_name + (f" → {issue.column_name}" if issue.column_name else "") + "</p>" if issue.table_name else ""}
                {"<div style='margin-top: 0.75rem; padding: 0.75rem; background: #F0FDF4; border-radius: 8px; border: 1px solid #BBF7D0;'><span style='color: #166534; font-size: 0.8125rem;'>💡 " + issue.suggestion + "</span></div>" if issue.suggestion else ""}
            </div>
            """, unsafe_allow_html=True)
    
    if report.warnings:
        with st.expander(f"⚠️ Warnings ({len(report.warnings)})", expanded=False):
            for issue in report.warnings:
                st.markdown(f"""
                <div class="premium-card issue-warning" style="margin-bottom: 0.75rem; padding: 1rem;">
                    <code style="background: #FEF3C7; color: #92400E; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem;">{issue.code}</code>
                    <span style="color: #0F172A; font-size: 0.875rem; margin-left: 0.5rem;">{issue.message}</span>
                </div>
                """, unsafe_allow_html=True)
    
    if report.info_items:
        with st.expander(f"ℹ️ Information ({len(report.info_items)})", expanded=False):
            for issue in report.info_items:
                st.markdown(f"""
                <div class="premium-card issue-info" style="margin-bottom: 0.75rem; padding: 1rem;">
                    <code style="background: #E0E7FF; color: #3730A3; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem;">{issue.code}</code>
                    <span style="color: #0F172A; font-size: 0.875rem; margin-left: 0.5rem;">{issue.message}</span>
                </div>
                """, unsafe_allow_html=True)


def render_tables(report):
    """Render table breakdown"""
    if not report.table_assessments:
        return
    
    st.markdown("""
    <h3 style="font-size: 1.125rem; font-weight: 600; color: #0F172A; margin: 2rem 0 1rem 0;">
        Table Details
    </h3>
    """, unsafe_allow_html=True)
    
    for ta in report.table_assessments:
        if ta.can_auto_convert:
            badge = "✅ Auto"
            badge_bg = "#D1FAE5"
            badge_color = "#065F46"
        elif ta.readiness_score >= 50:
            badge = "⚠️ Review"
            badge_bg = "#FEF3C7"
            badge_color = "#92400E"
        else:
            badge = "🚫 Blocked"
            badge_bg = "#FEE2E2"
            badge_color = "#991B1B"
        
        with st.expander(f"**{ta.full_name}** — {ta.readiness_score:.0f}%"):
            col1, col2, col3 = st.columns(3)
            col1.metric("Columns", ta.column_count)
            col2.metric("Constraints", ta.constraint_count)
            col3.metric("Issues", len(ta.issues))
            
            if ta.issues:
                for issue in ta.issues:
                    if issue.severity == IssueSeverity.CRITICAL:
                        st.error(f"`{issue.code}` {issue.message}")
                    elif issue.severity == IssueSeverity.WARNING:
                        st.warning(f"`{issue.code}` {issue.message}")
                    else:
                        st.info(f"`{issue.code}` {issue.message}")
            else:
                st.success("✓ No issues")


def render_output(result, report):
    """Render converted DDL"""
    
    # Header only (download buttons moved to separate section)
    st.markdown("""
    <h3 style="font-size: 1.25rem; font-weight: 600; color: #0F172A; margin: 2rem 0 0.5rem 0;">
        ✨ Converted Iceberg DDL
    </h3>
    """, unsafe_allow_html=True)
    
    if not result.success:
        st.error(f"Conversion failed: {result.error_message}")
        return
    
    if result.ewi_count > 0:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%);
            border: 1px solid #F59E0B;
            border-radius: 12px;
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        ">
            <span style="font-size: 1.25rem;">⚠️</span>
            <div>
                <div style="font-weight: 600; color: #92400E;">
                    {result.tables_converted} table(s) converted with {result.ewi_count} EWI marker(s)
                </div>
                <div style="font-size: 0.8125rem; color: #B45309;">
                    Search for <code style="background: #FDE68A; padding: 0.1rem 0.4rem; border-radius: 4px;">!!!RESOLVE EWI!!!</code> in the output
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%);
            border: 1px solid #10B981;
            border-radius: 12px;
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        ">
            <span style="font-size: 1.25rem;">✅</span>
            <div style="font-weight: 600; color: #065F46;">
                Successfully converted {result.tables_converted} table(s) with no issues
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.code(result.iceberg_ddl, language="sql", line_numbers=True)


def render_download_section(report, result=None):
    """Render download buttons at the top of results - only show relevant buttons"""
    
    st.markdown("""
    <h3 style="font-size: 1.25rem; font-weight: 600; color: #0F172A; margin: 1.5rem 0 0.75rem 0;">
        📥 Downloads
    </h3>
    """, unsafe_allow_html=True)
    
    # Determine which buttons to show based on available data
    has_conversion = result is not None and result.success
    
    if has_conversion:
        # Show all three download buttons when conversion is complete
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if PDF_AVAILABLE:
                pdf_data = generate_assessment_pdf(report)
                st.download_button(
                    "📊 Assessment Report",
                    data=pdf_data,
                    file_name="db2ice_assessment_report.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    help="Download the migration readiness assessment as PDF"
                )
            else:
                st.info("PDF export not available in this environment")
        
        with col2:
            st.download_button(
                "📄 Iceberg DDL (SQL)",
                data=result.iceberg_ddl,
                file_name="iceberg_tables.sql",
                mime="text/plain",
                use_container_width=True,
                help="Download the converted Snowflake Iceberg DDL"
            )
        
        with col3:
            if PDF_AVAILABLE:
                pdf_data = generate_conversion_pdf(result, report)
                st.download_button(
                    "📑 Full Report (PDF)",
                    data=pdf_data,
                    file_name="db2ice_conversion_report.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    help="Download complete conversion report with DDL as PDF"
                )
            else:
                st.info("PDF export not available")
    else:
        # Only show assessment download when just assessment is done
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if PDF_AVAILABLE:
                pdf_data = generate_assessment_pdf(report)
                st.download_button(
                    "📊 Assessment Report",
                    data=pdf_data,
                    file_name="db2ice_assessment_report.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    help="Download the migration readiness assessment as PDF"
                )
            else:
                st.info("PDF export not available in this environment")
        
        with col2:
            st.markdown("""
            <div style="
                background: #F1F5F9;
                border-radius: 8px;
                padding: 0.5rem 1rem;
                text-align: center;
                color: #94A3B8;
                font-size: 0.875rem;
            ">
                <span style="opacity: 0.7;">📄</span> Run "Convert" for DDL
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div style="
                background: #F1F5F9;
                border-radius: 8px;
                padding: 0.5rem 1rem;
                text-align: center;
                color: #94A3B8;
                font-size: 0.875rem;
            ">
                <span style="opacity: 0.7;">📑</span> Run "Convert" for full report
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("<div style='height: 0.5rem'></div>", unsafe_allow_html=True)


def main():
    # Initialize
    if 'input_ddl' not in st.session_state:
        st.session_state.input_ddl = ""
    
    # Header
    render_header()
    
    # Determine current step
    has_results = 'last_report' in st.session_state
    has_conversion = 'last_result' in st.session_state
    
    if has_conversion:
        current_step = 2
    elif has_results:
        current_step = 1
    else:
        current_step = 0
    
    # Steps indicator
    render_steps(current_step)
    
    st.markdown("<div style='height: 1.5rem'></div>", unsafe_allow_html=True)
    
    # Documentation (shown as overlay when requested)
    render_documentation()
    
    # Input
    ddl = render_input_card()
    
    # Config
    render_config_panel()
    
    st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
    
    # Actions
    assess, convert = render_action_buttons()
    
    # Process
    if assess or convert:
        if not ddl.strip():
            st.error("Please enter DDL first")
            st.stop()
        
        source_type = st.session_state.get('source_type', 'db2')
        
        if source_type == 'db2':
            # DB2 to Iceberg conversion
            with st.spinner("Analyzing DB2 DDL..."):
                assessor = Assessor()
                report = assessor.assess(ddl)
                st.session_state.last_report = report
            
            if convert:
                with st.spinner("Converting to Iceberg..."):
                    converter = DB2IceConverter(
                        external_volume=st.session_state.get('external_volume', 'my_iceberg_volume'),
                        base_location_pattern=st.session_state.get('base_location', '{schema}/{table}'),
                        include_comments=st.session_state.get('include_comments', True),
                        include_ewi=st.session_state.get('include_ewi', True)
                    )
                    result = converter.convert(ddl)
                    st.session_state.last_result = result
        else:
            # Snowflake Standard to Iceberg conversion
            with st.spinner("Analyzing Snowflake DDL..."):
                # Create a lightweight assessment for Snowflake DDL
                sf_converter = SnowflakeToIcebergConverter(
                    external_volume=st.session_state.get('external_volume', 'my_iceberg_volume'),
                    base_location_pattern=st.session_state.get('base_location', '{schema}/{table}'),
                    include_comments=st.session_state.get('include_comments', True),
                    include_ewi=st.session_state.get('include_ewi', True)
                )
                result = sf_converter.convert(ddl)
                
                # Create assessment report from Snowflake conversion
                report = create_snowflake_assessment_report(result, ddl)
                st.session_state.last_report = report
            
            if convert:
                st.session_state.last_result = result
        
        st.rerun()
    
    # Results
    if 'last_report' in st.session_state:
        report = st.session_state.last_report
        result = st.session_state.get('last_result', None)
        
        # Download section at the top - shows only relevant buttons
        render_download_section(report, result)
        
        render_score_visualization(report)
        render_stats(report)
        render_issues(report)
        render_tables(report)
        
        if result is not None:
            render_output(result, report)
    
    # Footer
    st.markdown("""
    <div style="text-align: center; padding: 3rem 0 1rem 0; color: #94A3B8; font-size: 0.8125rem;">
        Built with Snowflake Cortex Code ❄️
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
