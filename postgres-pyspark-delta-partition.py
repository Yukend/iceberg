import time
from faker import Faker
from pyspark.sql.types import FloatType
from pyspark.sql.functions import dayofmonth, month, year, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType
import random
from pyspark.sql.functions import udf


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('Create Delta Table') \
    .config("spark.jars", "/home/ubuntu/Downloads/postgresql-42.6.0.jar") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

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

random_id_gen = udf(lambda: "PRAC"+str(random.randint(10000, 20000)))
patient_df = patient_df.withColumn("practitioner", random_id_gen())
patient_df.repartition(col("year"), col("month"), col("day"), col("diagnosis"), col("gender")).repartition(1000).write \
    .partitionBy("year", "month", "day", "diagnosis", "gender").mode(saveMode="overwrite").format("delta") \
    .option("overwriteSchema", "true").save("delta-partition-1000/patients")
delta_patient = spark.read.format("delta").load("delta-partition-1000/patients")
patient_df.createOrReplaceTempView("delta_patient")

fake = Faker()
practitioner_schema = StructType([
    StructField("practitioner_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("specialty", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("location", StringType(), True),
    StructField("email", StringType(), True)
])

num_practitioners = 10000
practitioners = []
for id, _ in enumerate(range(num_practitioners)):
    practitioner_id = "PRAC" + str(num_practitioners + id)
    name = fake.name()
    specialty = fake.random.choice(
        ["Cardiologist", "Dermatologist", "Pediatrician", "Orthopedic Surgeon", "Psychiatrist"])
    experience_years = float(fake.random.randrange(1, 20))
    address = fake.address().replace('n', '').replace(',', '')
    email = fake.email()
    practitioners.append((practitioner_id, name, specialty,
                         experience_years, address, email))

practitioner_df = spark.createDataFrame(
    practitioners, schema=practitioner_schema)
practitioner_df.write.mode(saveMode="overwrite").format(
    "delta").save("delta-partition-1000/practitioners")
delta_practitioner = spark.read.format(
    "delta").load("delta-partition-1000/practitioners")
practitioner_df.createOrReplaceTempView("delta_practitioner")

s = time.time()
spark.sql("SELECT * FROM delta_patient WHERE admission_date REGEXP '01-21'").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT * FROM delta_patient WHERE admission_date REGEXP '2020-01-21' and diagnosis='Fever' and gender='Male' and first_name='Steven'").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT * FROM delta_patient WHERE admission_date REGEXP '01-21' and diagnosis='Fever' and gender='Male'").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT diagnosis, COUNT(diagnosis) as diagnosis_count FROM delta_patient GROUP BY diagnosis").show()
print(time.time() - s)
s = time.time()
spark.sql("SELECT * FROM delta_patient INNER JOIN delta_practitioner ON delta_practitioner.practitioner_id=delta_patient.practitioner").show()
print(time.time() - s)
