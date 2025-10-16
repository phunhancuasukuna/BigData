from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("HPG_Bai2_RDD").getOrCreate()
sc = spark.sparkContext

df = (spark.read.option("multiLine", True).json("hdfs:///input/hpg.json")
      .select("ThayDoi"))

rdd = df.rdd

def label(rec):
    s = rec["ThayDoi"]
    if not s: return None
    # Dạng ví dụ: "-1.05(-3.52 %)" hoặc "0.65(2.36 %)"
    m = re.match(r"\s*([-+]?\d*\.?\d+)", s)
    if not m: return None
    v = float(m.group(1))
    if v > 0:  return ("Tăng", 1)
    if v < 0:  return ("Giảm", 1)
    return ("Không đổi", 1)

mv = rdd.map(label).filter(lambda x: x is not None).reduceByKey(lambda a,b: a+b)
total = mv.map(lambda x: x[1]).reduce(lambda a,b: a+b)
percent = mv.map(lambda x: (x[0], round(x[1]*100.0/total, 2)))

out = "hdfs:///output/hpg_2"
spark.createDataFrame(percent, ["LoaiBienDong","PhanTram"]) \
     .coalesce(1).write.mode("overwrite").parquet(f"{out}/percentage")

spark.stop()
