from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructField, StringType, StructType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[2]") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
            .config('spark.cassandra.connection.host', "localhost")\
            .config('spark.cassandra.connection.port', "9042")\
            .appName("cust-data-stream")\
            .getOrCreate()

    schema = StructType([
                StructField("fname", StringType() ),
                StructField("lname", StringType()),
                StructField("url", StringType()),
                StructField("product", StringType()),
                StructField("cnt", IntegerType())

    ])

    custDf = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "custdata") \
        .option("stringOffsets", "earliest") \
        .load()

    # custDf.printSchema()

    custStringDf = custDf.select(col("value").cast("string"))

    custStringDf = custStringDf\
        .withColumn("fname", split(col("value"), ",")[0])\
        .withColumn("lname", split(col("value"), ",")[1])\
        .withColumn("url", split(col("value"), ",")[2])\
        .withColumn("product", split(col("value"), ",")[3])\
        .withColumn("cnt", split(col("value"), ",")[4])\
        .drop(col("value"))
    # custStringDf.printSchema()


    custDataToCassandra = custStringDf.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="cust_data", keyspace="cust_data_stream") \
        .option("checkpointLocation", "chk-pt-loc")\
        .outputMode("append") \
        .start()





    custDataToCassandra.awaitTermination()

