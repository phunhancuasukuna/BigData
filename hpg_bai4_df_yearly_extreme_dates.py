from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, max as smax, min as smin

spark = SparkSession.builder.appName("HPG_Bai4_DF").getOrCreate()

df = (spark.read.option("multiLine", True).json("hdfs:///input/hpg.json")
      .select("Ngay","GiaDongCua")
      .withColumn("Date", to_date(col("Ngay"), "dd/MM/yyyy"))
      .withColumn("Nam",  year(col("Date")))
      .withColumn("GiaDongCua", col("GiaDongCua").cast("double"))
      .cache())

ext = (df.groupBy("Nam")
         .agg(smax("GiaDongCua").alias("GiaCaoNhat"),
              smin("GiaDongCua").alias("GiaThapNhat")))

# join để lấy ngày tương ứng
max_dates = (df.alias("d").join(ext.alias("e"),
                     (col("d.Nam")==col("e.Nam")) & (col("d.GiaDongCua")==col("e.GiaCaoNhat")), "inner")
             .select(col("d.Nam").alias("Nam"),
                     col("d.Ngay").alias("NgayCaoNhat"),
                     col("d.GiaDongCua").alias("GiaCaoNhat")).distinct())

min_dates = (df.alias("d").join(ext.alias("e"),
                     (col("d.Nam")==col("e.Nam")) & (col("d.GiaDongCua")==col("e.GiaThapNhat")), "inner")
             .select(col("d.Nam").alias("Nam"),
                     col("d.Ngay").alias("NgayThapNhat"),
                     col("d.GiaDongCua").alias("GiaThapNhat")).distinct())

result = (max_dates.alias("mx").join(min_dates.alias("mn"), "Nam", "inner")
          .orderBy("Nam"))

out = "hdfs:///output/hpg_4"
result.coalesce(1).write.mode("overwrite").parquet(f"{out}/yearly_extreme_dates")
spark.stop()
