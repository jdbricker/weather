from pyspark import pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StringType
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
            'winter_season_start',
            F.when(
                (F.col('month') >= 11),
                F.make_date(F.col('year'), F.lit(11), F.lit(1))
            )
            .when(
                (F.col('month') <= 4),
                F.make_date(F.col('year') - 1 , F.lit(11), F.lit(1))
            )
            .otherwise(F.lit(None))
        )
        .withColumn(
            'winter_season',
            F.concat_ws('-', F.year('winter_season_start').cast(T.StringType()), (F.year('winter_season_start') + F.lit(1)).cast(T.StringType()))
        )
        .withColumn('winter_season_day', F.date_diff('date', 'winter_season_start'))
        .filter(F.col('winter_season_start').isNotNull())
        .withColumn(
            'cumulative_snowfall',
            F.sum(F.col('value') / 25.4)
            .over(
                Window
                .partitionBy(F.col('winter_season'))
                .orderBy(F.col('winter_season_day'))
            )
        )
        .select('winter_season',  'winter_season_day','year','month','day', 'cumulative_snowfall')
    )
    
    return df