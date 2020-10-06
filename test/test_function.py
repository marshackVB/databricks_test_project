from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.types import *
import pytest


from databricks_test_project.create_data import generate_dataframe
from databricks_test_project.pyspark_function import transformation_function



@pytest.fixture(scope='session')
def spark_context():
    spark = SparkSession.builder.appName("Unit Tests").getOrCreate()
    return spark


def test_1(spark_context):

    # Input test dataset
    input_schema = [StructField('account',      IntegerType(), True),
                    StructField('company_name', StringType(),  True),
                    StructField('account_type', StringType(),  True),
                    StructField('balance',      StringType(),  True)]

    input_data = [[100, 200, 300],
                      ['company_a', 'company_b', 'company_c'],
                      ['a', 'b', 'c'],
                      [1, 2, 3]]

    input_df = generate_dataframe(input_data, input_schema)


    # Output test dataset
    output_schema = [StructField('company_name', StringType(),  True),
                    StructField('sum',          IntegerType(),  True)]

    output_data = [['company_a', 'company_b', 'company_c'],
                   [1, 2, 4]]

    output_df = generate_dataframe(output_data, output_schema)


    # Function output
    test_df = transformation_function(input_df, 'company_name', 'balance')

    # Compare
    differences = test_df.subtract(output_df).count()

    assert differences == 0