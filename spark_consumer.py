from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# -----------------------------------------
# MYSQL CONFIG
# -----------------------------------------
MYSQL_URL = "jdbc:mysql://localhost:3306/energy_db"
MYSQL_TABLE = "energy_weather_data"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Root@123"   # change if different

# -----------------------------------------
# SPARK SESSION
# -----------------------------------------
spark = SparkSession.builder \
    .appName("KafkaToMySQLConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------------------
# KAFKA SOURCE
# -----------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "energy_consumption") \
    .option("startingOffsets", "latest") \
    .load()

# Convert bytes to string
df = df.selectExpr("CAST(value AS STRING)")

# -----------------------------------------
# SCHEMA (ENERGY + WEATHER COMBINED)
# -----------------------------------------
schema = StructType([
    StructField("DateTime", StringType(), True),
    StructField("Global_active_power", DoubleType(), True),
    StructField("Global_reactive_power", DoubleType(), True),
    StructField("Voltage", DoubleType(), True),
    StructField("Global_intensity", DoubleType(), True),
    StructField("Sub_metering_1", DoubleType(), True),
    StructField("Sub_metering_2", DoubleType(), True),
    StructField("Sub_metering_3", DoubleType(), True),

    # WEATHER FIELDS
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("city", StringType(), True),
])

# Parse JSON
parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# -----------------------------------------
# WRITE FUNCTION TO MYSQL
# -----------------------------------------
def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .mode("append") \
        .save()

# -----------------------------------------
# STREAM TO MYSQL
# -----------------------------------------
query = parsed_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

query.awaitTermination()