from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, avg, count, when, hour, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import os

os.makedirs("output", exist_ok=True)

# Creating a spark session

spark = (SparkSession.builder
.appName("Fraud Detection")
.getOrCreate()
)

# so first import the main psyarpk tool, spark session is entry point, then import specific functions we need to use later

# spark session builder makes the session so connection to the spark engine, the app name gives the job a name

# get or creta makes a new session or reuses one

# the \ is a lien continuation character, so we can write the code on multiple lines for readability


# ==========================================================
# DATA QUALITY CHECKS (ADDED)
# ==========================================================

df = spark.read.csv('fraudTest.csv', header=True, inferSchema=True)

# reading the csv file into a spark dataframe, we specify the path to the file, and some options

df.printSchema()

# check nulls per column (added improvement)

df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()


# ==========================================================
# BASIC CLEANING
# ==========================================================

df = df.drop("_c0")

# remove duplicates (added improvement)
df = df.dropDuplicates()


# ==========================================================
# DATE/TIME FEATURE ENGINEERING
# ==========================================================

#Split date and time into separate column

df = df.withColumn("trans_date", split(col('trans_date_trans_time'), " ")[0])
df = df.withColumn("trans_time", split(col('trans_date_trans_time'), " ")[1])

# spark read csv it reads the csv file into a spark data frame, header true means first row is column names, infer scvhema is

# so datatypes are figured out, df with column adds a new column to the dataframe, takes name of new column and what to put in

# split col, spilts that combined column with two parts, the split is on the  space in the actual values


# NEW: timestamp column for better analysis
df = df.withColumn(
    "timestamp",
    to_timestamp(col("trans_date_trans_time"))
)

df.show(5)




# ==========================================================
# FRAUD RATE BY CATEGORY
# ==========================================================


fraud_by_category = (df.groupBy("category")
.agg(
count("is_fraud").alias("total_transactions"),
avg("is_fraud").alias("fraud_rate")
)
.withColumn("fraud_rate", (col("fraud_rate") * 100).cast("decimal(5,2)"))
.orderBy(col("fraud_rate").desc())
)

# NEW: window ranking (added improvement)
window_spec = Window.orderBy(col("fraud_rate").desc())

fraud_by_category = fraud_by_category.withColumn(
    "rank",
    rank().over(window_spec)
)

fraud_by_category.show()


# ==========================================================
# FRAUD RATE BY AMOUNT RANGE
# ==========================================================

# when() is PySpark's IF statement - buckets transactions into ranges

fraud_by_amount = (df
.withColumn("amount_range",
when(col("amt") < 100, "0-100")
.when(col("amt") < 500, "100-500")
.when(col("amt") < 1000, "500-1000")
.otherwise("1000+")
)
.groupBy("amount_range")
.agg(
count("is_fraud").alias("total_transactions"),
avg("is_fraud").alias("fraud_rate")
)
.withColumn("fraud_rate", (col("fraud_rate") * 100).cast("decimal(5,2)"))
.orderBy(col("fraud_rate").desc())
)

fraud_by_amount.show()


# ==========================================================
# FRAUD RATE BY HOUR (ADDED INSIGHT)
# ==========================================================

fraud_by_hour = (
df.groupBy(hour(col("timestamp")).alias("hour"))
.agg(
count("is_fraud").alias("total_transactions"),
avg("is_fraud").alias("fraud_rate")
)
.withColumn("fraud_rate", (col("fraud_rate") * 100).cast("decimal(5,2)"))
.orderBy("hour")
)

fraud_by_hour.show()


# ==========================================================
# OUTPUTS
# ==========================================================

# Write results to CSV using pandas

# In production this would be:

# fraud_by_category.write.mode("overwrite").parquet("s3://bucket/output/fraud_by_category")

fraud_by_category.toPandas().to_csv("output/fraud_by_category.csv", index=False)
fraud_by_amount.toPandas().to_csv("output/fraud_by_amount.csv", index=False)
fraud_by_hour.toPandas().to_csv("output/fraud_by_hour.csv", index=False)

# NOTE (important improvement explained in interview):
# toPandas collects data to driver memory -> not scalable for big data

print("Pipeline complete. Results written to output folder.")


# ==========================================================
# STOP SPARK (ADDED BEST PRACTICE)
# ==========================================================

spark.stop()