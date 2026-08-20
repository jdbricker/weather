from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

SOURCE = 'silver_weather'

@dp.materialized_view(
    name="gold_snwd",
    comment="snow depth"
)
def snwd():

    df = (
        spark
        .table(SOURCE)
        .filter(F.col("element") == 'SNWD')
        .withColumn('depth_in', F.col('value')/F.lit(25.4))
    )
    
    return df

