from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

SOURCE = 'data.silver.weather'

@dp.table(
    name="data.gold.snwd",
    comment="snow depth"
)
def snwd():

    df = (
        spark
        .readStream
        .table(SOURCE)
        .filter(F.col("element") == 'SNWD')
        .withColumn('depth_in', F.col('value')/F.lit(25.4))
    )
    
    return df

