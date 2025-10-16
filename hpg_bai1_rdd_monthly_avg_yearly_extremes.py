from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("HPG_Bai1_RDD").getOrCreate()
sc = spark.sparkContext

# JSON của bạn là mảng -> dùng multiLine=True
df = (spark.read.option("multiLine", True).json("hdfs:///input/hpg.json")
      .select("Ngay", "GiaDongCua"))

rdd = df.rdd

def parse(rec):
    try:
        if not rec["Ngay"] or rec["GiaDongCua"] is None: return None
        d = datetime.strptime(rec["Ngay"], "%d/%m/%Y")
        return (d.year, d.month, float(rec["GiaDongCua"]))
    except Exception:
        return None

parsed = rdd.map(parse).filter(lambda x: x is not None).cache()

# TB theo tháng
monthly_avg = (parsed
    .map(lambda x: ((x[0], x[1]), (x[2], 1)))
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    .map(lambda kv: (kv[0][0], kv[0][1], round(kv[1][0]/kv[1][1], 4)))
    .sortBy(lambda x: (x[0], x[1])))

# Cao/Thấp nhất theo năm
year_minmax = (parsed
    .map(lambda x: (x[0], x[2]))
    .groupByKey()
    .map(lambda kv: (kv[0], round(max(kv[1]),4), round(min(kv[1]),4)))
    .sortBy(lambda x: x[0]))

monthly_avg_df = spark.createDataFrame(monthly_avg, ["Nam","Thang","GiaDongCua_TB"])
yearly_ext_df = spark.createDataFrame(year_minmax, ["Nam","GiaDongCua_CaoNhat","GiaDongCua_ThapNhat"])

out = "hdfs:///output/hpg_1"
monthly_avg_df.write.mode("overwrite").parquet(f"{out}/monthly_avg")
yearly_ext_df.write.mode("overwrite").parquet(f"{out}/yearly_extremes")

spark.stop()
