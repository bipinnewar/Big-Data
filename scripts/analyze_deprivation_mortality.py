from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# Initialize Spark session
spark = SparkSession.builder.appName("AggregateDeprivationMortality").getOrCreate()

# Load cleaned datasets from HDFS
imd_df = spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/cleaned/imd_cleaned.csv", header=True, inferSchema=True)
ons_df = spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/cleaned/ons_cleaned.csv", header=True, inferSchema=True)

# Filter for 2020 and Avoidable mortality
ons_df = ons_df.filter((col("Year") == 2020))

# Aggregate IMD data by decile
imd_agg = imd_df.groupBy("IMD_Decile") \
    .agg(round(avg("IMD_Score"), 2).alias("Mean_IMD_Score")) \
    .orderBy(col("IMD_Decile").asc())

# Aggregate ONS data by decile (average across sexes)
ons_agg = ons_df.groupBy("IMD_Decile") \
    .agg(round(avg("Mortality_Rate"), 2).alias("Avg_Mortality_Rate")) \
    .orderBy(col("IMD_Decile").asc())

# Join aggregated datasets on IMD_Decile
merged_df = imd_agg.join(ons_agg, "IMD_Decile", "inner") \
    .select("IMD_Decile", "Mean_IMD_Score", "Avg_Mortality_Rate") \
    .orderBy("IMD_Decile")

# Compute Pearson correlation
correlation = merged_df.corr("Mean_IMD_Score", "Avg_Mortality_Rate")
print("Pearson Correlation between Mean_IMD_Score and Avg_Mortality_Rate: {}".format(correlation))

# Coalesce to single partition for one output file
merged_df = merged_df.coalesce(1)

# Save merged dataset to HDFS
merged_df.write.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/processed", header=True, mode="overwrite")
print("Merged dataset saved to /user/BigDataProject/data/processed")

# Stop Spark session
spark.stop()