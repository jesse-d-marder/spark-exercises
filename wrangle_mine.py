from pyspark.sql.functions import *


def get_311_data(spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    print("[wrangle.py] reading case.csv")
    df = spark.read.csv("case.csv", header = True, inferSchema = True)
    
    
def wrangle_311(spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    df = add_features(handle_dates(handle_dtypes(get_311_data(spark))))
    return join_departments(df, spark)
    
if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    df = wrangle_311(spark)