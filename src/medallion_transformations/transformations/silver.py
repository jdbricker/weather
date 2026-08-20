from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

SOURCE = 'bronze_weather'  # Same schema (jdbricker_dev), different table

# Stage 1: Append-only streaming table with all parsed records
@dp.table(
    name="weather_staging",
    comment="Staging table for parsed NOAA weather data (append-only, may contain duplicates)"
)
def weather_staging():
    # Read and filter raw data
    df = (
        spark
        .readStream
        .table(SOURCE)
        .filter(F.col("raw_text").isNotNull())
        .filter(F.trim(F.col("raw_text")) != "")
    )
    
    # Trim and extract fixed-width header fields in one operation
    df = df.withColumns({
        "raw_text": F.trim(F.col("raw_text")),
        "station_id": F.substring("raw_text", 1, 11),
        "year": F.substring("raw_text", 12, 4).cast(IntegerType()),
        "month": F.substring("raw_text", 16, 2).cast(IntegerType()),
        "element": F.substring("raw_text", 18, 4)
    })
    
    # Extract 31 daily values (each is 8 characters starting at position 22)
    daily_values = [F.substring("raw_text", 22 + (day - 1) * 8, 8) for day in range(1, 32)]
    df = df.withColumn("daily_values", F.array(*daily_values))
    
    # Explode to get one row per day
    df = df.select("*", F.posexplode("daily_values").alias("day_idx", "day_string"))
    
    # Parse all day-level fields in one operation
    df = df.withColumns({
        "day": F.col("day_idx") + 1,
        "value": F.substring("day_string", 1, 5).cast(IntegerType()),
        "mflag": F.substring("day_string", 6, 1),
        "qflag": F.substring("day_string", 7, 1),
        "sflag": F.substring("day_string", 8, 1),
        "date": F.make_date("year", "month", F.col("day_idx") + 1)
    })
    
    # Filter out missing values (-9999) and invalid dates
    df = (
        df
        .filter(F.col("value") != -9999)
        .filter(F.col("date").isNotNull())
    )
    
    # Select final columns
    return df.select(
        "station_id",
        "date",
        "element",
        "value",
        "mflag",
        "qflag",
        "sflag",
        "source_file",
        "ingested_at"
    )

# Stage 2: Deduplicated table with MERGE logic - keeps LATEST record per station/date/element
# Define the target table first (required by apply_changes)
@dp.table(
    name="silver_weather",
    comment="Deduplicated weather observations (SCD Type 1 - latest record per station/date/element)"
)
def weather():
    return None  # apply_changes manages the table lifecycle

# Using apply_changes for CDC-style upsert (SCD Type 1)
dp.apply_changes(
    target="silver_weather",
    source="weather_staging",
    keys=["station_id", "date", "element"],
    sequence_by="ingested_at",
    stored_as_scd_type=1,  # SCD Type 1: only keep latest version
    column_list=[
        "station_id",
        "date",
        "element",
        "value",
        "mflag",
        "qflag",
        "sflag",
        "source_file",
        "ingested_at"
    ]
)


