from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HCM_Bai2_RDD").getOrCreate()
df = spark.read.option("multiLine", True).json("hdfs:///input/hcm_cleaned.json").select("prcp")
rdd = df.rdd

def classify(rec):
    p = rec["prcp"]
    if p is None: return None
    v = float(p)
    if v == 0: return ("No rain",1)
    elif v < 10: return ("Light",1)
    elif v < 50: return ("Moderate",1)
    else: return ("Heavy",1)

counts = rdd.map(classify).filter(lambda x:x).reduceByKey(lambda a,b:a+b)
total = counts.map(lambda x:x[1]).sum()
result = counts.map(lambda x:(x[0], x[1], round(x[1]*100.0/total,2)))

spark.createDataFrame(result,["Type","Count","Percent"])\
     .coalesce(1).write.mode("overwrite").parquet("hdfs:///output/hcm_2/rain_share")
spark.stop()
