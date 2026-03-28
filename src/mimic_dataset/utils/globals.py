from pyspark.sql import SparkSession

class GlobalVariables:
    spark: SparkSession = None

    @classmethod
    def setup_spark(cls):
        GlobalVariables.spark = SparkSession.builder.getOrCreate()
