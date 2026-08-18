# Weather Pipeline - Declarative Automation Bundle

This project processes NOAA weather data through a medallion architecture (Bronze → Silver → Gold) using Databricks Spark Declarative Pipelines.

## Project Structure

```
weather/
├── databricks.yml                    # DAB configuration
├── resources/
│   └── pipelines.yml                # Pipeline resource definitions
├── medallion_transformations/
│   ├── transformations/
│   │   ├── bronze.py               # Raw data ingestion
│   │   ├── silver.py               # Parsed & cleaned data
│   │   └── gold.py                 # Aggregated data
│   ├── explorations/               # Ad-hoc analysis (gitignored)
│   └── .gitignore
└── fetch_data.py                   # Data fetching utility
```

## Data Pipeline

### Bronze Layer
- **Source**: Text files from `/Volumes/data/default/data/weather/`
- **Method**: Auto Loader
- **Output**: `data.bronze.weather`

### Silver Layer
- **Input**: `data.bronze.weather`
- **Transformation**: Parses NOAA fixed-width format
- **Output**: `data.silver.weather`

### Gold Layer
- **Input**: `data.silver.weather`
- **Transformation**: Filters specific elements
- **Output**: Element-specific tables

## Configuration

Edit `databricks.yml` to customize:
- `catalog`: Unity Catalog name
- `schema`: Schema for tables

## Targets
- **dev**: Development (default)
- **prod**: Production
