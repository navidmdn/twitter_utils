from pyspark.sql import SparkSession


CORES = 8
DRIVER_MEM = 6
MAX_RESULT_SIZE = 4


def get_spark(cores=CORES, driver_mem=DRIVER_MEM, max_res_size=MAX_RESULT_SIZE):
    spark = SparkSession.builder \
        .appName("spark")\
        .master(f"local[{cores}]")\
        .config("spark.driver.memory", f"{driver_mem}G")\
        .config("spark.driver.maxResultSize", f"{max_res_size}g") \
        .getOrCreate()
    return spark
