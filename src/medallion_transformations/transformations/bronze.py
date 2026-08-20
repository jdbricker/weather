from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE = '/Volumes/data/default/data/weather/'

@dp.table(
    name="bronze_weather",
    comment="Raw weather data ingested from text files using Auto Loader"
)
def bronze():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(SOURCE)
        .withColumns({
            "raw_text": F.col("value"),
            "source_file": F.col("_metadata.file_path"),
            "ingested_at": F.current_timestamp()
        })
        .drop("value")
    )
