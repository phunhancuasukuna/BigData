from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, max as smax, min as smin
spark = SparkSession.builder.appName("HCM_Bai4_DF").getOrCreate()
df = (spark.read.option("multiLine", True).json("hdfs:///input/hcm_cleaned.json")
      .select("time","tmax","tmin")
      .withColumn("Date", to_date(col("time"), "yyyy-MM-dd"))
      .withColumn("Year", year(col("Date")))
      .cache())
ext = (df.groupBy("Year")
        .agg(smax("tmax").alias("Max_tmax"),
             smin("tmin").alias("Min_tmin")))
max_dates = (df.alias("d").join(ext.alias("e"),
    (col("d.Year")==col("e.Year")) & (col("d.tmax")==col("e.Max_tmax")), "inner")
    .select(col("d.Year"),col("d.time").alias("Hot_Date"),col("d.tmax")))
min_dates = (df.alias("d").join(ext.alias("e"),
    (col("d.Year")==col("e.Year")) & (col("d.tmin")==col("e.Min_tmin")), "inner")
    .select(col("d.Year"),col("d.time").alias("Cold_Date"),col("d.tmin")))
res = max_dates.join(min_dates,"Year","inner").orderBy("Year")
res.coalesce(1).write.mode("overwrite").parquet("hdfs:///output/hcm_4/yearly_extreme_dates")
spark.stop()
