from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType, TimestampType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

spark_jars_packages = ",".join(
        [
            kafka_lib_id,
        ]
    )

# настройки security для кафки
# вы можете использовать их с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    spark = SparkSession.builder\
        .master("local")\
        .appName('PrintSchema')\
        .config("spark.jars.packages", spark_jars_packages)\
        .getOrCreate()

    return spark


def load_df(spark: SparkSession) -> DataFrame:
    df = (spark.read
       .format('kafka')\
       .option('kafka.bootstrap.servers','rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')\
       .options(**kafka_security_options)\
       .option("subscribe", "persist_topic")\
       .load())

    return df


def transform(df: DataFrame) -> DataFrame:

    schema = StructType([
        StructField("subscription_id", IntegerType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("key", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("topic", StringType(), nullable=True),
        StructField("partition", IntegerType(), nullable=True),
        StructField("offset", LongType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("timestampType", IntegerType(), nullable=True),
    ])

    # Using select
    df = df.select(f.col("value").cast(StringType()).alias("value_str"))\
           .withColumn("deserialized_value", f.from_json(f.col("value_str"), schema=schema))\
           .select("deserialized_value.*")

    return df

spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)


df.printSchema()
df.show(truncate=False)
