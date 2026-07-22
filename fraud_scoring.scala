import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Load fraud transaction data
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("fraudTest.csv")

df.printSchema()

// Bucket transactions into amount ranges, then compute fraud rate per state + amount range
val fraudByStateAmount = df
  .withColumn("amount_range",
    when(col("amt") < 100, "0-100")
      .when(col("amt") < 500, "100-500")
      .when(col("amt") < 1000, "500-1000")
      .otherwise("1000+")
  )
  .groupBy("state", "amount_range")
  .agg(
    count("is_fraud").alias("total_transactions"),
    avg("is_fraud").alias("fraud_rate")
  )
  .withColumn("fraud_rate", (col("fraud_rate") * 100).cast("decimal(5,2)"))

// Rank amount ranges within each state by fraud rate (partitioned window function)
// Note: rank().over(...) triggered a literal-type resolution issue in Spark 4.2 / Scala 2.13;
// using expr() with SQL window syntax as a stable workaround
val fraudByStateAmountRanked = fraudByStateAmount
  .withColumn("rank_within_state", expr("rank() over (partition by state order by fraud_rate desc)"))

// Show the #1 riskiest amount range per state
fraudByStateAmountRanked.filter(col("rank_within_state") === 1)
  .orderBy("state")
  .show(20)