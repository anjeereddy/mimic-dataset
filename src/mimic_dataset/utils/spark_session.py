from pyspark.sql import SparkSession

_spark = None

def get_spark():
    global _spark

    if _spark is None:
        _spark = SparkSession.builder.getOrCreate()

    return _spark