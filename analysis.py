from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("HW2").getOrCreate()

df = spark.read.csv("selected_columns.csv",header=True,inferSchema=True)

df.show(5)

df.printSchema()

df.select("Brnd_Name","Tot_Spndng_2023").show(5)

df2 = df.withColumn(
    "Spending_Change",
    col("Tot_Spndng_2023") - col("Tot_Spndng_2019")
)

df2.show(5)

high_cost=df2.filter(col("Avg_Spnd_Per_Clm_2023")>1000)
high_cost.show(5)

df2.coalesce(1).write.csv("output",header=True,mode="overwrite")

spark.stop()
