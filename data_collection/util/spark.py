from pyspark.sql import SparkSession


CORES = 8
DRIVER_MEM = 6
MAX_RESULT_SIZE = 4

spark = SparkSession.builder \
    .appName("spark")\
    .master(f"local[{CORES}]")\
    .config("spark.driver.memory", f"{DRIVER_MEM}G")\
    .config("spark.driver.maxResultSize", f"{MAX_RESULT_SIZE}g") \
    .getOrCreate()
