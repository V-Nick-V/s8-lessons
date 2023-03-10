from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType

#создаём SparkSession
spark = SparkSession.builder \
    .appName("count words") \
    .getOrCreate()

#определяем схему для датафрейма
userSchema = StructType([StructField("text", StringType(), True)])

#читаем текст из файла
wordsDF = spark.readStream.schema(userSchema).format('text').load('/datas8')

#разделяем слова по запятым
splitWordsDF = wordsDF.select(explode(split(wordsDF.text, ",")).alias("word"))

#группируем слова
wordCountsDF = splitWordsDF.groupBy("word").count()

# запускаем стриминг
wordCountsDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start() \
    .awaitTermination() 
