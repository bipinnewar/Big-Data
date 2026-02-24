from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("CleanONS2020").getOrCreate()

# Load raw ONS 2020 CSV from HDFS, skipping 4 header rows
ons_df = spark.read.option("header", "false").option("skipRows", 4).csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/raw/ons2020.csv")

# Manually assign column names
ons_df = ons_df.toDF("Measure", "Year", "Sex", "Decile", "Deaths", "Rate", "Lower CI", "Upper CI")

# Filter for 2020 and Avoidable mortality
ons_filtered = ons_df.filter((col("Measure") == "Avoidable mortality"))

# Select relevant columns, cast Decile and Rate
ons_cleaned = ons_filtered.select(
    col("Measure"),
    col("Year"),
    col("Decile").cast("int").alias("IMD_Decile"),
    col("Rate").cast("double").alias("Mortality_Rate")
).filter(col("Mortality_Rate").isNotNull())

# Coalesce to single partition for one output file
ons_cleaned = ons_cleaned.coalesce(1)

# Save cleaned dataset to HDFS
ons_cleaned.write.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/cleaned/ons", header=True, mode="overwrite")

# Print confirmation
print("Cleaned ONS data saved to /user/BigDataProject/data/cleaned/ons")

# Stop Spark session
spark.stop()