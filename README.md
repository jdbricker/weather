# Weather Pipeline - Declarative Automation Bundle

This project processes NOAA weather data through a medallion architecture (Bronze → Silver → Gold) using Databricks Spark Declarative Pipelines.

## Project Structure

```
weather/
├── databricks.yml                    # DAB configuration
├── resources/                        # Resource definitions
│   ├── jobs.yml                     # Job definitions (coordinated workflow)
│   └── pipelines.yml                # Pipeline resource definitions
├── src/                              # Source code
│   ├── fetch_data.py                # Data fetching script
│   └── medallion_transformations/   # Pipeline transformations
│       ├── transformations/
│       │   ├── bronze.py           # Raw data ingestion (Auto Loader)
│       │   ├── silver.py           # Parsed & cleaned data
│       │   └── gold.py             # Aggregated data
│       ├── explorations/           # Ad-hoc analysis (gitignored)
│       └── .gitignore
├── .gitignore                        # DAB artifacts
└── README.md                         # Documentation
```

## Workflow Architecture

The project uses a **coordinated job workflow** that runs on a schedule:

```
┌─────────────────────┐
│  Scheduled Job      │  (Daily at 2 AM UTC)
│  (weather_data_     │
│   workflow)         │
└──────┬──────────────┘
       │
       ├─► Task 1: fetch_weather_data
       │   └─ Runs fetch_data.py
       │   └─ Downloads NOAA data → /Volumes/data/default/data/weather/
       │
       └─► Task 2: process_weather_data (depends on Task 1)
           └─ Triggers weather_medallion pipeline
           └─ Bronze → Silver → Gold
```

## Data Pipeline Layers

### Bronze Layer
- **Source**: Text files from `/Volumes/data/default/data/weather/`
- **Method**: Auto Loader (detects new files automatically)
- **Output**: `data.bronze.weather`
- **Schema**: `raw_text`, `source_file`, `ingested_at`

### Silver Layer
- **Input**: `data.bronze.weather`
- **Transformation**: Parses NOAA GHCN-Daily fixed-width format
- **Output**: `data.silver.weather`
- **Schema**: `station_id`, `date`, `element`, `value`, quality flags
- **Process**: Extracts station metadata, explodes 31 daily values per row

### Gold Layer
- **Input**: `data.silver.weather`
- **Transformation**: Filters specific weather elements (TMAX, TMIN, PRCP, SNWD, etc.)
- **Output**: Element-specific tables (e.g., `data.gold.snwd` for snow depth)
- **Enhancement**: Unit conversions, aggregations

## Configuration

Edit `databricks.yml` to customize:
- `catalog`: Unity Catalog name (default: `workspace`)
- `schema`: Schema for tables (default: `default` for dev)

## Targets
- **dev**: Development (default)
  - Personal workspace
  - Development mode enabled
- **prod**: Production
  - Shared workspace
  - Service principal execution

## Deployment

Validate and deploy the entire workflow:

```bash
cd /Workspace/Users/<your-email>/weather
databricks bundle validate
databricks bundle deploy -t dev
```

The job will start running on its schedule automatically. To trigger manually:

```bash
databricks bundle run weather_data_workflow -t dev
```

## NOAA Data Format

Processes NOAA GHCN-Daily fixed-width format:
- Positions 1-11: Station ID
- Positions 12-15: Year
- Positions 16-17: Month
- Positions 18-21: Element type (TMAX, TMIN, PRCP, SNWD, etc.)
- Positions 22+: 31 daily values × 8 chars each (5 for value + 3 for flags)

Missing values coded as `-9999`.
