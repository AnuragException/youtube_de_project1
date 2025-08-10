import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("anurag_spark2")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .config("spark.jars", "D:\\data_engg_test\\Manish_Kumar_DE_Project\\youtube_de_project1\\mysql-connector-j-9.3.0.jar") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark