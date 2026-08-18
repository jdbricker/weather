from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

SOURCE = 'data.bronze.weather'

@dp.table(
    name="data.silver.weather",
    comment="Parsed NOAA fixed-width weather data in long format with one row per station/date/element"
)
def weather():
    # Read and filter raw data
    df = (
        spark
        .readStream
        .table(SOURCE)
        .filter(F.col("raw_text").isNotNull())
        .filter(F.trim(F.col("raw_text")) != "")
        .withColumn("raw_text", F.trim(F.col("raw_text")))
    )
    
    # Extract fixed-width header fields
    df = (
        df
        .withColumn("station_id", F.substring("raw_text", 1, 11))
        .withColumn("year", F.substring("raw_text", 12, 4).cast(IntegerType()))
        .withColumn("month", F.substring("raw_text", 16, 2).cast(IntegerType()))
        .withColumn("element", F.substring("raw_text", 18, 4))
    )
    
    # Extract 31 daily values (each is 8 characters starting at position 22)
    daily_values = []
    for day in range(1, 32):
        start_pos = 22 + (day - 1) * 8
        daily_values.append(F.substring("raw_text", start_pos, 8))
    
    df = df.withColumn("daily_values", F.array(*daily_values))
    
    # Explode to get one row per day
    df = df.select("*", F.posexplode("daily_values").alias("day_idx", "day_string"))
    df = df.withColumn("day", F.col("day_idx") + 1)
    
    # Parse value (first 5 chars) and flags (remaining 3 chars)
    df = (
        df
        .withColumn("value", F.substring("day_string", 1, 5).cast(IntegerType()))
        .withColumn("mflag", F.substring("day_string", 6, 1))
        .withColumn("qflag", F.substring("day_string", 7, 1))
        .withColumn("sflag", F.substring("day_string", 8, 1))
    )
    
    # Create date column from year, month, day
    df = df.withColumn("date", F.make_date("year", "month", "day"))
    
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


