from pyspark import pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

SOURCE = 'weather'

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

@dp.materialized_view(
    name="gold_cumulative_snowfall",
    comment="cumulative snowfall by season "
)
def snowfall():

    df = (
        spark
        .table(SOURCE)
        .filter(F.col('element') == 'SNOW')
        .withColumn('day', F.dayofmonth(F.col('date')))
        .withColumn('month', F.month(F.col('date')))
        .withColumn('year', F.year(F.col('date')))
        .withColumn('next_year', F.col('year') + 1)
        .withColumn('previous_year', F.col('year') - 1)
        .withColumn(
            'winter_season',
            F.when(
                (F.col('month') >= 11),
                F.concat_ws('-', F.col('year').cast(T.StringType()), F.col('next_year').cast(T.StringType()))
            )
            .when(
                (F.col('month') <= 5),
                F.concat_ws('-', F.col('previous_year').cast(T.StringType()), F.col('year').cast(T.StringType()))
            )
            .otherwise(F.lit(None))
        )
        .filter(F.col('winter_season').isNotNull())
        .withColumn(
            'cumulative_snowfall',
            F.sum(F.col('value') / 25.4)
            .over(
                Window
                .partitionBy(F.col('winter_season'))
                .orderBy(F.col('month'), F.dayofmonth(F.col('date')))
            )
        )
        .select('winter_season','year','month','day', 'cumulative_snowfall')
    )
    
    return df