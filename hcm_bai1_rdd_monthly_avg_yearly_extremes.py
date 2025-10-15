from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month
from pyspark.sql.types import DoubleType
spark = SparkSession.builder.appName("HCM_Bai1_RDD").getOrCreate()
df = (spark.read.option("multiLine", True)
      .json("hdfs:///input/hcm_cleaned.json")
      .select("time", "tavg", "tmax", "tmin"))
df = (df.withColumn("Date", to_date(df["time"], "yyyy-MM-dd"))
        .withColumn("Year", year("Date"))
        .withColumn("Month", month("Date"))
        .withColumn("tavg", df["tavg"].cast(DoubleType()))
        .withColumn("tmax", df["tmax"].cast(DoubleType()))
        .withColumn("tmin", df["tmin"].cast(DoubleType())))
df = df.filter(df.tavg.isNotNull() & df.tmax.isNotNull() & df.tmin.isNotNull())
rdd = df.rdd.map(lambda row: ((row["Year"], row["Month"]),
                              (row["tavg"], row["tmax"], row["tmin"], 1))) \
             .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))
rdd_avg = rdd.map(lambda x: (x[0][0], x[0][1],
                             round(x[1][0]/x[1][3], 2),
                             round(x[1][1]/x[1][3], 2),
                             round(x[1][2]/x[1][3], 2)))
columns = ["Year", "Month", "Avg_tavg", "Avg_tmax", "Avg_tmin"]
df_avg = spark.createDataFrame(rdd_avg, columns)
df_avg.coalesce(1).write.mode("overwrite").parquet("hdfs:///output/hcm_1/monthly_avg")
spark.stop()
