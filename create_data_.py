from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()

def generate_dataframe(data, schema):

    return spark.createDataFrame(zip(*data), schema = StructType(schema))