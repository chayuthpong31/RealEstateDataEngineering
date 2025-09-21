import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json, col
from pyspark import SparkContext

CASSANDRA_HOST = "cassandra-new"

def create_keyspace_and_table():
    cluster = Cluster(contact_points=[CASSANDRA_HOST], port=9042)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS property_streams
        WITH replication = {'class':'SimpleStrategy', 'replication_factor':'1'};
    """)
    print("Keyspace created successfully!")

    session.set_keyspace("property_streams")
    session.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            address text,
            link text,
            pictures list<text>,
            title text,
            price int,
            bedrooms int,
            bathrooms int,
            room_size int,
            receptions int,  
            EPC_Rating text,  
            tenure text,    
            service_charge text,   
            concil_tax_band text, 
            commonhold_detail text,        
            ground_rent text,
            PRIMARY KEY (link)           
        );
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    session.execute("""
        INSERT INTO properties(
            address,
            link,
            pictures,
            title,
            price,
            bedrooms,
            bathrooms,
            room_size,
            receptions,  
            EPC_Rating,  
            tenure,    
            service_charge,   
            concil_tax_band, 
            commonhold_detail,        
            ground_rent
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, tuple(kwargs.values()))


def write_to_cassandra(batch_df, batch_id):
    cluster = Cluster(contact_points=[CASSANDRA_HOST], port=9042)
    session = cluster.connect("property_streams")

    for row in batch_df.collect():   # ดึงข้อมูลจาก batch
        insert_data(session, **row.asDict())

def main():
    logging.basicConfig(level=logging.INFO)

    # สร้าง keyspace + table ก่อนเริ่ม stream
    create_keyspace_and_table()
    # ตรวจสอบว่า context เดิมมีอยู่ และยังไม่ถูก stop
    # try:
    #     sc = SparkContext.getOrCreate()
    #     if not sc._jsc.sc().isStopped():
    #         sc.stop()
    # except Exception as e:
    #     # ถ้าไม่มี context เดิม ก็ไม่ทำอะไร
    #     print(e)
    #     pass

    spark = (
        SparkSession.builder
        .appName("RealEstateConsumer")
        .config("spark.cassandra.connection.host", "cassandra-new")
        .config("spark.jars.ivy", "/tmp/.ivy2")  # absolute path สำหรับ Ivy
        .config("spark.ui.enabled", "false")             # ปิด metrics UI กัน error
        .config("spark.metrics.conf.*.sink.servlet.class", "")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.metrics.conf.*.source.jvm.class", "")   # ปิด JVM metrics
        .config("spark.metrics.conf.*.sink.graphite.class", "")     # ปิด graphite
        .config("spark.metrics.conf.*.sink.csv.class", "")          # ปิด csv
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1"
        )
        .getOrCreate()
    )

    # อ่านจาก Kafka
    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "kafka-broker:9092")
                .option("subscribe", "properties")
                .option("startingOffsets", "earliest")
                .load())

    schema = StructType([
        StructField("address", StringType(), True),
        StructField("link", StringType(), True),
        StructField("pictures", ArrayType(StringType()), True),
        StructField("title", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("room_size", IntegerType(), True),
        StructField("receptions", IntegerType(), True),
        StructField("EPC_Rating", StringType(), True),
        StructField("tenure", StringType(), True),
        StructField("service_charge", StringType(), True),
        StructField("concil_tax_band", StringType(), True),
        StructField("commonhold_detail", StringType(), True),
        StructField("ground_rent", StringType(), True)
    ])

    kafka_df = (kafka_df
                .selectExpr("CAST(value AS STRING) as value")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*"))

    # เขียนเข้า Cassandra
    (kafka_df.writeStream
        .foreachBatch(write_to_cassandra)
        .start()
        .awaitTermination())

if __name__ == "__main__":
    main()
