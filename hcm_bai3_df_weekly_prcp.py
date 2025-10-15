from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, weekofyear, sum as ssum

spark = SparkSession.builder.appName("HCM_Bai3_DF").getOrCreate()

df = (spark.read
      .option("multiLine", True)
      .option("mode", "PERMISSIVE")
      .json("hdfs:///input/hcm_cleaned.json")
      .select("time","prcp")
      .withColumn("Date", to_date(col("time"), "yyyy-MM-dd"))
      .withColumn("Year", year(col("Date")))
      .withColumn("Week", weekofyear(col("Date")))
      .withColumn("prcp", col("prcp").cast("double")))

weekly = (df.groupBy("Year","Week")
          .agg(ssum("prcp").alias("Sum_prcp"))
          .orderBy("Year","Week"))

weekly.coalesce(1).write.mode("overwrite").parquet("hdfs:///output/hcm_3/weekly_prcp")
spark.stop()
