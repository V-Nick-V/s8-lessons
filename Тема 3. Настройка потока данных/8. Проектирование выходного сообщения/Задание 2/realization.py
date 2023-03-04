from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, FloatType
from pyspark.sql.functions import udf
from math import radians, cos, sin, asin, sqrt

TOPIC_NAME = "student.topic.cohort4.NickVlasov"

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

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
}

def get_distance(longit_a, latit_a, longit_b, latit_b):
    # Transform to radians
    #print (f'{longit_a}, {latit_a}, {longit_b}, {latit_b}')
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  
                                                            latit_a, 
                                                         longit_b, 
                                                         latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    # Calculate area
    area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    # Calculate Distance
    distance = central_angle * radius
    return abs(round(distance, 2))

def spark_init(test_name) -> SparkSession:
    spark = SparkSession.builder\
        .master("local")\
        .appName(test_name)\
        .config("spark.jars.packages", spark_jars_packages)\
        .getOrCreate()

    return spark

def read_marketing(spark: SparkSession) -> DataFrame:
    df = (spark.read
        .format("jdbc") \
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de") \
        .option("dbtable", "public.marketing_companies") \
        .options(**postgresql_settings)\
        .option("driver", "org.postgresql.Driver")\
        .load())

    return df

def read_client_stream(spark: SparkSession) -> DataFrame:

    schema = StructType([
        StructField("client_id", StringType(), nullable=True),
        StructField("timestamp", DoubleType(), nullable=True),
        StructField("lat", DoubleType(), nullable=True),
        StructField("lon", DoubleType(), nullable=True),
    ])

    df = (spark.readStream
    .format('kafka')\
       .option('kafka.bootstrap.servers','rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')\
       .options(**kafka_security_options)\
       .option("subscribe", TOPIC_NAME)\
       .load())

    df = df.select(f.col("value").cast(StringType()).alias("value_str"))\
           .withColumn("deserialized_value", f.from_json(f.col("value_str"), schema=schema))\
           .select("deserialized_value.client_id", 
                   f.from_unixtime(f.col("deserialized_value.timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").cast(TimestampType()).alias("timestamp"),
                   "deserialized_value.lat",
                   "deserialized_value.lon")\
           .dropDuplicates(["client_id", "timestamp"])\
           .withWatermark("timestamp", "1 millisecond")

    return df

udf_get_distance = f.udf(get_distance, FloatType())

def join(user_df, marketing_df) -> DataFrame:
    df = user_df.crossJoin(marketing_df)\
        .withColumn("distance", udf_get_distance("lat","lon","point_lat","point_lon"))\
        .filter(f.col("distance")<1)

    return df

if __name__ == "__main__":
    spark = spark_init('join stream')

    client_stream = read_client_stream(spark)

    marketing_df = read_marketing(spark)

    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()

   