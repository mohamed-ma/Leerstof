from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,sum,count,when

spark = SparkSession.builder.config("spark.driver.cores", 2).appName("Spark_les").getOrCreate()
df = spark.read.option("delimiter", ",").option("header", True).csv("/user/bigdata/06_Spark/demo/input.csv")

# rename columns with a dot
df.printSchema()
df = df.drop("_c0")

df.show(5)

df.select(avg("sepal length (cm)") ).show()
df.select(avg("sepal length (cm)").alias("avg sepal length") ).show()
# df.select(avg("sepal length (cm)"), avg("sepal width (cm)")).show()


#df.select([avg(c) for c in df.columns]).show()
# collect gaat dit lokaal behouden en op die manier kunnen we die variabele dan gebruiken bij df_larger_than_avg
df_avg = df.select([avg(c).alias("avg " + c) for c in df.columns] ).collect()[0]
#df_avg.show()

df.select([col(c) > 3 for c in df.columns]).show(2)

# enumerate zal een index bijhouden, hence de index
df_larger_than_avg = df.select([(df[c] > df_avg[index]).alias(c) for index,c in enumerate(df.columns)])
# df_larger_than_avg.select([sum(col(c).cast("int")) for c in df.columns]).show()
# df_larger_than_avg.select([count(when(col(c), 1)) for c in df.columns]).show()

# tmp = df_larger_than_avg.select([(col(c).cast("int")).alias(c) for c in df_larger_than_avg.columns])
# c = df_larger_than_avg.columns
# df2 = tmp.withColumn("aantal", col(c[0]) + col(c[1]) + col(c[2]) + col(c[3]) + col(c[4]))
# df2.show(3)

# df3 = df.withColumn("class", when(col("target") == 0, "klasse 0")
#                      .when(col("target") ==1, "klasse 1")
#                      .otherwise("klasse 2"))
# df3.show(5)
# df3.tail(5)

# df3.filter(col("target") ==2).show()
