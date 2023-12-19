import time
from faker import Faker
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
import random
from pyspark.sql.functions import dayofmonth, month, year
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]") \
    .appName("Create Iceberg Table") \
    .config("spark.jars", "/home/ubuntu/Downloads/postgresql-42.6.0.jar") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "./iceberg-partition-1000") \
    .config("spark.hadoop.iceberg.writer.spec-id", "-1") \
    .getOrCreate()


# Define the schema for the data
schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("admission_date", DateType(), True),
    StructField("discharged", BooleanType(), True)
])
patient_df = spark.read.format("csv").option("delimeter", ",").option(
    "header", True).schema(schema).load("data.csv")

patient_df = patient_df.withColumn("month", month("admission_date"))
patient_df = patient_df.withColumn("year", year("admission_date"))
patient_df = patient_df.withColumn("day", dayofmonth("admission_date"))


random_id_gen = udf(lambda: "PRAC"+str(random.randint(10000, 99999)))
patient_df = patient_df.withColumn("practitioner", random_id_gen())
patient_df.repartition(1000).writeTo("local.patient_iceberg").createOrReplace()
spark.sql("ALTER TABLE local.patient_iceberg ADD PARTITION FIELD year")
spark.sql("ALTER TABLE local.patient_iceberg ADD PARTITION FIELD month")
spark.sql("ALTER TABLE local.patient_iceberg ADD PARTITION FIELD day")
spark.sql("ALTER TABLE local.patient_iceberg ADD PARTITION FIELD diagnosis")
spark.sql("ALTER TABLE local.patient_iceberg ADD PARTITION FIELD gender")

fake = Faker()

practitioner_schema = StructType([
    StructField("practitioner_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("specialty", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("location", StringType(), True),
    StructField("email", StringType(), True)
])

# Generate Fake Practitioners
num_practitioners = 10000

practitioners = []
for id, _ in enumerate(range(num_practitioners)):
    practitioner_id = "PRAC" + str(num_practitioners + id)
    name = fake.name()
    specialty = fake.random.choice(
        ["Cardiologist", "Dermatologist", "Pediatrician", "Orthopedic Surgeon", "Psychiatrist"])
    experience_years = float(fake.random.randrange(1, 20))
    address = fake.address().replace('\n', '').replace(',', '')
    email = fake.email()

    practitioners.append((practitioner_id, name, specialty,
                         experience_years, address, email))

# Create DataFrame from practitioners
practitioner_df = spark.createDataFrame(
    practitioners, schema=practitioner_schema)
practitioner_df.writeTo("local.practitioner_iceberg").createOrReplace()

s = time.time()
spark.sql(
    "SELECT * FROM local.patient_iceberg WHERE admission_date REGEXP '01-21'").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT * FROM local.patient_iceberg WHERE admission_date REGEXP '2020-01-21' and diagnosis='Fever' and gender='Male' and first_name='Steven'").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT * FROM local.patient_iceberg WHERE admission_date REGEXP '01-21' and diagnosis='Fever' and gender='Male'").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT diagnosis, COUNT(diagnosis) as diagnosis_count FROM local.patient_iceberg GROUP BY diagnosis").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT * FROM local.patient_iceberg INNER JOIN local.practitioner_iceberg on local.practitioner_iceberg.practitioner_id=local.patient_iceberg.practitioner").show()
print(time.time() - s)
