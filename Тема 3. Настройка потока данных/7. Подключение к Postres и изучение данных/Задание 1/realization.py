from pyspark.sql import SparkSession

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}    

spark = SparkSession.builder\
    .master("local")\
    .appName('test connect to postgres')\
    .config("spark.jars.packages", spark_jars_packages)\
    .getOrCreate()

df = (spark.read
        .format("jdbc") \
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de") \
        .option("dbtable", "public.marketing_companies") \
        .options(**postgresql_settings)\
        .option("driver", "org.postgresql.Driver")\
        .load())

print("rows=",df.count())

df.printSchema()

df.select("*").show(truncate=False)
df.select("start_time", "end_time").printSchema()