# DB2ICE - IBM DB2 or Snowflake Standard (FDN) Tables DDL to Snowflake Managed Iceberg Converter

<div align="center">

**Convert IBM DB2 or Snowflake Standard table DDL to Snowflake Managed Iceberg tables**

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-4A90D9?style=for-the-badge&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)

[Features](#features) | [Deployment](#deployment) | [Data Type Mappings](#data-type-mappings) | [EWI Codes](#ewi-code-reference)

</div>

---

## Overview

**DB2ICE** is a migration assessment and DDL conversion tool that transforms table DDL from **IBM DB2** or **Snowflake Standard (FDN)** tables into **Snowflake Managed Iceberg** table DDL.

### Key Capabilities

| Feature | Description |
|---------|-------------|
| **Dual Source Support** | Convert from IBM DB2 DDL or Snowflake Standard tables |
| **Intelligent Type Mapping** | 30+ data types mapped with Iceberg compatibility |
| **Readiness Assessment** | Traffic-light scoring (Green/Yellow/Red) |
| **EWI Markers** | SnowConvert-style inline markers for issues |
| **Table Type Handling** | Proper handling of TEMPORARY, TRANSIENT, DYNAMIC, EXTERNAL, HYBRID |
| **Built-in Documentation** | In-app reference for all mappings and behaviors |

---

## Features

### Core Features
- **Assessment Mode** - Analyze DDL and get readiness score without conversion
- **Conversion Mode** - Generate Iceberg DDL with EWI markers
- **Dual Source Support** - IBM DB2 or Snowflake Standard as input
- **Sample DDL** - Pre-loaded samples for both source types
- **In-App Documentation** - Complete reference for mappings and EWI codes

### Assessment & Reporting
- **Readiness Scoring** - Traffic-light system (Green ≥80%, Yellow 50-79%, Red <50%)
- **Component Breakdown** - Separate scores for data types, constraints, partitions, special features
- **Per-Table Analysis** - Individual assessment for each table
- **PDF Report Generation** - Downloadable assessment reports
- **DDL Export** - Download converted DDL files

### EWI (Error/Warning/Issue) System
- **SnowConvert-Style Markers** - `!!!RESOLVE EWI!!! /*** SSC-EWI-xxx - message ***/!!!`
- **Severity Levels** - Critical (blocks), Warning (review), Info (notes)
- **Actionable Suggestions** - Each issue includes resolution guidance

---

## Deployment

### Option 1: Run Locally

#### Prerequisites

- Python 3.9 or higher
- pip package manager

#### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/db2ice.git
cd db2ice

# Install dependencies
pip install streamlit fpdf2

# Run the application
streamlit run app.py
```

The app will open automatically in your browser at **http://localhost:8501**

#### Local Configuration (Optional)

Create `.streamlit/config.toml` to customize the theme:

```toml
[theme]
primaryColor = "#0EA5E9"
backgroundColor = "#FAFBFC"
secondaryBackgroundColor = "#F1F5F9"
textColor = "#1E293B"

[server]
port = 8501
```

---

### Option 2: Deploy to Snowflake (Streamlit in Snowflake)

Deploy DB2ICE directly to your Snowflake account as a native Streamlit app.

#### Prerequisites

- Snowflake account with Streamlit enabled
- `ACCOUNTADMIN` role or privileges to create stages and Streamlit apps
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) installed (recommended)

#### Method A: Using Snowflake CLI (Recommended)

**Step 1: Configure Snowflake Connection**

```bash
# Add a connection (if not already configured)
snow connection add

# Test the connection
snow connection test -c <your_connection_name>
```

**Step 2: Deploy with Snow CLI**

The project includes a `snowflake.yml` file for easy deployment:

```bash
cd db2ice

# Deploy the Streamlit app (creates stage and uploads files automatically)
snow streamlit deploy --replace -c <your_connection_name>

# Get the app URL
snow streamlit get-url DB2ICE -c <your_connection_name>
```

#### Method B: Manual Deployment via Snowsight

**Step 1: Create a Stage**

Run in a Snowflake worksheet:

```sql
-- Set context
USE ROLE ACCOUNTADMIN;  -- Or role with CREATE STAGE privilege
USE DATABASE your_database;
USE SCHEMA your_schema;

-- Create a stage for the app files
CREATE OR REPLACE STAGE DB2ICE_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for DB2ICE Streamlit app';
```

**Step 2: Upload Files**

Using SnowSQL:

```bash
# Connect to Snowflake
snowsql -a <account> -u <username>

# Upload main app file
PUT file:///path/to/db2ice/app.py @your_database.your_schema.DB2ICE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

# Upload environment file
PUT file:///path/to/db2ice/environment.yml @your_database.your_schema.DB2ICE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

# Upload db2ice module files
PUT file:///path/to/db2ice/db2ice/__init__.py @your_database.your_schema.DB2ICE_STAGE/db2ice/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/db2ice/db2ice/parser.py @your_database.your_schema.DB2ICE_STAGE/db2ice/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/db2ice/db2ice/mapper.py @your_database.your_schema.DB2ICE_STAGE/db2ice/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/db2ice/db2ice/converter.py @your_database.your_schema.DB2ICE_STAGE/db2ice/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/db2ice/db2ice/assessor.py @your_database.your_schema.DB2ICE_STAGE/db2ice/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/db2ice/db2ice/snowflake_converter.py @your_database.your_schema.DB2ICE_STAGE/db2ice/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Or upload via Snowsight UI:
1. Go to **Data** → **Databases** → Select your database/schema
2. Click on the **DB2ICE_STAGE** stage
3. Click **+ Files** and upload all files maintaining the folder structure

**Step 3: Create the Streamlit App**

```sql
CREATE OR REPLACE STREAMLIT your_database.your_schema.DB2ICE
  ROOT_LOCATION = '@your_database.your_schema.DB2ICE_STAGE'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'your_warehouse'
  TITLE = 'DB2ICE - DDL to Iceberg Converter'
  COMMENT = 'Convert DB2 or Snowflake Standard DDL to Iceberg tables';
```

**Step 4: Grant Access to Other Users (Optional)**

```sql
-- Grant access to specific roles
GRANT USAGE ON STREAMLIT your_database.your_schema.DB2ICE TO ROLE analyst_role;
GRANT USAGE ON STREAMLIT your_database.your_schema.DB2ICE TO ROLE data_engineer_role;
```

**Step 5: Access the App**

1. In Snowsight, go to **Streamlit** in the left navigation
2. Find and click on **DB2ICE**
3. The app will open in a new tab

#### Method C: Using Snowsight UI (No CLI Required)

**Step 1: Create Streamlit App in Snowsight**

1. In Snowsight, click **Streamlit** in the left navigation
2. Click **+ Streamlit App**
3. Enter app name: `DB2ICE`
4. Select database, schema, and warehouse
5. Click **Create**

**Step 2: Replace Default Code**

1. In the Streamlit editor, delete the default code
2. Copy the entire contents of `app.py` and paste it
3. Click the **Packages** tab and add: `fpdf2`

**Step 3: Add Supporting Files**

1. In the Streamlit editor, click **Add file** or the folder icon
2. Create a folder named `db2ice`
3. Add each file from the `db2ice/` directory:
   - `__init__.py`
   - `parser.py`
   - `mapper.py`
   - `converter.py`
   - `assessor.py`
   - `snowflake_converter.py`

**Step 4: Run the App**

Click **Run** to start the application.

---

### Deployment Troubleshooting

| Issue | Solution |
|-------|----------|
| "Module not found: db2ice" | Ensure all files in `db2ice/` folder are uploaded with correct paths |
| "No module named fpdf" | Add `fpdf2` to packages in Snowsight or `environment.yml` |
| Stage upload fails | Check network/firewall settings; try Snowsight UI upload instead |
| Permission denied | Ensure you have CREATE STREAMLIT privilege on the schema |
| Warehouse suspended | Ensure the query warehouse is running |

---

## Usage

1. **Select Source Type** - Choose "IBM DB2" or "Snowflake Standard"
2. **Paste DDL** - Enter your `CREATE TABLE` statements (or use "Try Sample")
3. **Configure** - Set external volume name and base location pattern
4. **Assess** - Click "Assess Readiness" for migration report
5. **Convert** - Click "Convert to Iceberg" to generate DDL
6. **Review** - Check EWI markers and resolve critical issues
7. **Download** - Export DDL and assessment report

---

## Data Type Mappings

### IBM DB2 → Snowflake Iceberg

#### Supported Types (Direct Mapping)

| DB2 Type | Iceberg Type | Notes |
|----------|--------------|-------|
| `SMALLINT` | `SMALLINT` | Direct mapping |
| `INTEGER` / `INT` | `INTEGER` | Direct mapping |
| `BIGINT` | `BIGINT` | Direct mapping |
| `DECIMAL(p,s)` / `NUMERIC` | `NUMBER(p,s)` | Up to precision 38 |
| `REAL` | `FLOAT` | Single precision |
| `DOUBLE` | `DOUBLE` | Double precision |
| `DATE` | `DATE` | Direct mapping |
| `BOOLEAN` | `BOOLEAN` | Direct mapping |
| `VARCHAR(n)` | `VARCHAR(n)` | Direct mapping |
| `BINARY(n)` | `BINARY(n)` | Direct mapping |
| `VARBINARY(n)` | `VARBINARY(n)` | Direct mapping |

#### Supported with Adjustments (EWI Generated)

| DB2 Type | Iceberg Type | Adjustment |
|----------|--------------|------------|
| `CHAR(n)` | `VARCHAR(n)` | Iceberg doesn't support fixed-length CHAR |
| `TIME` | `TIME(6)` | Precision forced to 6 (microseconds) |
| `TIMESTAMP(p)` | `TIMESTAMP_NTZ(6)` | Precision forced to 6 (microseconds) |
| `CLOB` | `VARCHAR` | Size limit 128MB |
| `BLOB` | `BINARY` | Size limit 128MB |
| `LONG VARCHAR` | `VARCHAR` | Converted to variable length |
| `GRAPHIC(n)` | `VARCHAR(n*4)` | DBCS to Unicode conversion |
| `VARGRAPHIC(n)` | `VARCHAR(n*4)` | DBCS to Unicode conversion |
| `DBCLOB` | `VARCHAR` | DBCS to Unicode, size limit |
| `DECFLOAT` | `DOUBLE` | Loses decimal floating point precision |
| `ROWID` | `VARCHAR(40)` | System-generated, values not preserved |

#### Not Supported (Critical Issues)

| DB2 Type | Issue | Recommendation |
|----------|-------|----------------|
| `XML` | No Iceberg equivalent | Store as VARCHAR string or redesign |
| `EDITPROC` | Data transformation unknown | Review procedure, transform during ETL |
| `VALIDPROC` | Validation logic unknown | Implement in application layer |
| `FIELDPROC` | Column encryption | Decrypt before migration |
| `GENERATED ALWAYS` | Not supported | Compute values during ETL |

---

### Snowflake Standard → Snowflake Iceberg

#### Supported Types (Direct Mapping)

| Snowflake Type | Iceberg Type | Notes |
|----------------|--------------|-------|
| `NUMBER(p,s)` | `NUMBER(p,s)` | Preserved |
| `INT` / `INTEGER` | `NUMBER(10,0)` | 32-bit signed integer |
| `BIGINT` | `NUMBER(19,0)` | 64-bit signed integer |
| `FLOAT` / `DOUBLE` | `FLOAT` | 64-bit floating point |
| `VARCHAR(n)` | `VARCHAR(n)` | Max 128MB for Iceberg |
| `STRING` / `TEXT` | `VARCHAR` | Max 128MB |
| `BINARY` | `BINARY` | Preserved |
| `BOOLEAN` | `BOOLEAN` | Preserved |
| `DATE` | `DATE` | Preserved |

#### Timestamp Types (Precision Adjusted)

| Snowflake Type | Iceberg Type | Notes |
|----------------|--------------|-------|
| `TIME` | `TIME(6)` | Precision forced to 6 (μs) |
| `TIMESTAMP` | `TIMESTAMP_NTZ(6)` | Precision forced to 6 (μs) |
| `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ(6)` | Precision forced to 6 (μs) |
| `TIMESTAMP_LTZ` | `TIMESTAMP_LTZ(6)` | Precision forced to 6 (μs) |
| `TIMESTAMP_TZ` | `TIMESTAMP_LTZ(6)` | Converted to LTZ |

#### NOT Supported in Iceberg (Converted to VARCHAR)

| Snowflake Type | Converted To | Warning |
|----------------|--------------|---------|
| `VARIANT` | `VARCHAR` | Semi-structured not supported in Iceberg |
| `OBJECT` | `VARCHAR` | Use structured OBJECT with defined schema |
| `ARRAY` | `VARCHAR` | Use structured ARRAY with defined element type |
| `GEOGRAPHY` | `VARCHAR` | Store as WKT/GeoJSON string |
| `GEOMETRY` | `VARCHAR` | Store as WKT/GeoJSON string |

> **Important**: Iceberg tables do NOT support Snowflake's semi-structured types (VARIANT, OBJECT, ARRAY). Data must be serialized to VARCHAR or use Iceberg's structured types with predefined schemas.

---

## Table Type Handling

### IBM DB2 Source Tables

| DB2 Table Type | Converted To | Reason |
|----------------|--------------|--------|
| Regular `TABLE` | Iceberg Table | Full conversion with type mapping |
| `VOLATILE TABLE` | Snowflake `TEMPORARY` | Session-scoped, Iceberg doesn't support |
| `GLOBAL TEMPORARY` | Snowflake `TEMPORARY` | Session-scoped, Iceberg doesn't support |

### Snowflake Standard Source Tables

| Snowflake Table Type | Converted To | Reason |
|----------------------|--------------|--------|
| Regular `TABLE` | Iceberg Table | Full conversion |
| `TEMPORARY TABLE` | Kept as `TEMPORARY` | Session-scoped, Iceberg doesn't support |
| `TRANSIENT TABLE` | Kept as `TRANSIENT` | No Fail-safe needed, preserve behavior |
| `DYNAMIC TABLE` | Skipped | Auto-refresh from query - incompatible |
| `EXTERNAL TABLE` | Skipped | Already on external storage |
| `HYBRID TABLE` | Skipped | HTAP-optimized, different model |

---

## Iceberg Table Output Format

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
- `EXTERNAL_VOLUME` - Specifies the cloud storage location
- `BASE_LOCATION` - Path within the external volume

---

## EWI Code Reference

### DB2 Source EWI Codes

| Code | Severity | Description |
|------|----------|-------------|
| `SSC-EWI-DB2ICE-0001` | Warning | CHAR converted to VARCHAR |
| `SSC-EWI-DB2ICE-0003` | Info | TIME precision adjusted to 6 |
| `SSC-EWI-DB2ICE-0004` | Info | TIMESTAMP precision adjusted to 6 |
| `SSC-EWI-DB2ICE-0005` | Critical | XML type not supported |
| `SSC-EWI-DB2ICE-0007` | Warning | DECFLOAT precision loss |
| `SSC-EWI-DB2ICE-0011` | Critical | FIELDPROC detected |
| `SSC-EWI-DB2ICE-0012` | Critical | EDITPROC detected |
| `SSC-EWI-DB2ICE-0013` | Critical | VALIDPROC detected |
| `SSC-EWI-DB2ICE-0014` | Warning | GENERATED ALWAYS not supported |

### Snowflake Source EWI Codes

| Code | Severity | Description |
|------|----------|-------------|
| `SSC-EWI-SF2ICE-0001` | Critical | VARIANT not supported - converted to VARCHAR |
| `SSC-EWI-SF2ICE-0002` | Critical | Semi-structured OBJECT not supported |
| `SSC-EWI-SF2ICE-0003` | Critical | Semi-structured ARRAY not supported |
| `SSC-EWI-SF2ICE-0004` | Critical | GEOGRAPHY converted to VARCHAR |
| `SSC-EWI-SF2ICE-0005` | Critical | GEOMETRY converted to VARCHAR |
| `SSC-EWI-SF2ICE-0006` | Info | TIME precision adjusted to 6 |
| `SSC-EWI-SF2ICE-0007` | Info | TIMESTAMP precision adjusted to 6 |
| `SSC-EWI-SF2ICE-0008` | Info | TIMESTAMP_LTZ precision adjusted to 6 |
| `SSC-EWI-SF2ICE-0009` | Warning | TIMESTAMP_TZ converted to TIMESTAMP_LTZ |
| `SSC-EWI-SF2ICE-0015` | Warning | IDENTITY/AUTOINCREMENT not supported |
| `SSC-EWI-SF2ICE-0016` | Warning | MASKING POLICY needs re-application |
| `SSC-EWI-SF2ICE-0017` | Info | COLLATE clause not supported |
| `SSC-EWI-SF2ICE-0020` | Info | TEMPORARY table kept as Standard |
| `SSC-EWI-SF2ICE-0021` | Info | TRANSIENT table kept as Standard |
| `SSC-EWI-SF2ICE-0022` | Critical | DYNAMIC table cannot be converted |
| `SSC-EWI-SF2ICE-0023` | Critical | EXTERNAL table cannot be converted |
| `SSC-EWI-SF2ICE-0024` | Critical | HYBRID table cannot be converted |

---

## Project Structure

```
db2ice/
├── app.py                        # Streamlit UI (main entry point)
├── snowflake.yml                 # Snowflake CLI deployment config
├── environment.yml               # Snowflake package dependencies
├── README.md                     # This documentation
├── CONVERSION_SCENARIOS.md       # Detailed conversion examples
└── db2ice/
    ├── __init__.py               # Package exports
    ├── parser.py                 # DB2 DDL parser
    ├── mapper.py                 # DB2 → Iceberg type mapper
    ├── assessor.py               # Readiness assessment engine
    ├── converter.py              # DB2 → Iceberg converter
    └── snowflake_converter.py    # Snowflake Standard → Iceberg converter
```

---

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| External Volume | `my_iceberg_volume` | Snowflake external volume name |
| Base Location | `{schema}/{table}` | Pattern for Iceberg storage path |
| Include EWI | `true` | Add `!!!RESOLVE EWI!!!` markers |
| Include Comments | `true` | Add SQL comments for constraints |

---

## Example Conversions

### DB2 Input

```sql
CREATE TABLE SALES.ORDERS (
    ORDER_ID INTEGER NOT NULL,
    CUSTOMER_ID INTEGER NOT NULL,
    ORDER_DATE TIMESTAMP(9),
    TOTAL DECIMAL(15,2),
    NOTES CLOB(1M),
    PRIMARY KEY (ORDER_ID)
);
```

### Iceberg Output

```sql
-- Converted from DB2: SALES.ORDERS
CREATE OR REPLACE ICEBERG TABLE SALES.ORDERS (
    ORDER_ID INTEGER NOT NULL,
    CUSTOMER_ID INTEGER NOT NULL,
    ORDER_DATE TIMESTAMP_NTZ(6),
    TOTAL NUMBER(15,2),
    NOTES VARCHAR,
    PRIMARY KEY (ORDER_ID)
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'my_iceberg_volume'
BASE_LOCATION = 'sales/orders';
```

### Snowflake Standard Input

```sql
CREATE TABLE analytics.events (
    event_id INTEGER,
    event_data VARIANT,
    location GEOGRAPHY,
    created_at TIMESTAMP_NTZ(9)
);
```

### Iceberg Output

```sql
-- Converted from Snowflake Standard: analytics.events
CREATE OR REPLACE ICEBERG TABLE ANALYTICS.EVENTS (
    EVENT_ID INTEGER,
    EVENT_DATA VARCHAR
        !!!RESOLVE EWI!!! /*** SSC-EWI-SF2ICE-0001 - VARIANT not supported in Iceberg - converted to VARCHAR ***/!!!,
    LOCATION VARCHAR
        !!!RESOLVE EWI!!! /*** SSC-EWI-SF2ICE-0004 - GEOGRAPHY not supported in Iceberg - converted to VARCHAR ***/!!!,
    CREATED_AT TIMESTAMP_NTZ(6)
        !!!RESOLVE EWI!!! /*** SSC-EWI-SF2ICE-0007 - TIMESTAMP_NTZ precision adjusted to 6 ***/!!!
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'my_iceberg_volume'
BASE_LOCATION = 'analytics/events';
```

---

## Limitations

### Iceberg Data Type Limitations

1. **No Semi-Structured Types** - VARIANT, OBJECT (semi-structured), ARRAY (semi-structured) not supported
2. **Timestamp Precision** - Must be exactly 6 (microseconds)
3. **No Spatial Types** - GEOGRAPHY and GEOMETRY not supported
4. **LOB Size Limits** - Maximum 128MB for VARCHAR/BINARY
5. **No UUID** - UUID type not supported for Snowflake-catalog Iceberg tables

### Feature Limitations

1. **Constraints Not Enforced** - FOREIGN KEY, UNIQUE, CHECK are documented only
2. **No IDENTITY Columns** - Use sequences or compute during ETL
3. **No Clustering** - Iceberg uses automatic micro-partitioning
4. **Procedural Logic** - EDITPROC/VALIDPROC/FIELDPROC logic not analyzed

---

## Acknowledgments

- Built with [Snowflake Cortex Code](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- Inspired by [SnowConvert](https://docs.snowflake.com/en/migrations/snowconvert-docs) migration patterns
- Type mappings based on [Snowflake Iceberg Data Types](https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types)

---

<div align="center">

**Built with Snowflake Cortex Code**

</div>
