from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, weekofyear

spark = SparkSession.builder.appName("HPG_Bai3_DF").getOrCreate()

df = (spark.read.option("multiLine", True).json("hdfs:///input/hpg.json")
      .select("Ngay","KhoiLuongKhopLenh","KLThoaThuan"))

weekly = (df
    .withColumn("Date", to_date(col("Ngay"), "dd/MM/yyyy"))
    .withColumn("Nam", year(col("Date")))
    .withColumn("Tuan", weekofyear(col("Date")))
    .withColumn("KhoiLuongKhopLenh", col("KhoiLuongKhopLenh").cast("double"))
    .withColumn("KLThoaThuan",     col("KLThoaThuan").cast("double"))
    .withColumn("TongKhoiLuong", col("KhoiLuongKhopLenh") + col("KLThoaThuan"))
    .groupBy("Nam","Tuan").sum("TongKhoiLuong")
    .withColumnRenamed("sum(TongKhoiLuong)", "TongKhoiLuongGD")
    .orderBy("Nam","Tuan")
)

out = "hdfs:///output/hpg_3"
weekly.coalesce(1).write.mode("overwrite").parquet(f"{out}/weekly_volume")
spark.stop()
