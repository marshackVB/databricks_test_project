import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def transformation_function(df, groupby_col, value_col):
    """Groupby and get max value"""

    return df.groupby(groupby_col).agg(func.sum(value_col).alias('sum'))