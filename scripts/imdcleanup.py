from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("CleanIMD2019").getOrCreate()

# Load raw IMD 2019 CSV from HDFS
imd_df = spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/raw/imd2019.csv", header=True, inferSchema=True)

# Select relevant columns with correct names
imd_cleaned = imd_df.select(
    col("Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)").alias("IMD_Decile"),
    col("Index of Multiple Deprivation (IMD) Score").alias("IMD_Score")
).filter(col("IMD_Score").isNotNull())

# Coalesce to single partition for one output file
imd_cleaned = imd_cleaned.coalesce(1)

# Save cleaned dataset to HDFS
imd_cleaned.write.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/BigDataProject/data/cleaned/imd", header=True, mode="overwrite")

# Print confirmation
print("Cleaned IMD data saved to /user/BigDataProject/data/cleaned/imd")

# Stop Spark session
spark.stop()