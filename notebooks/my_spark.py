from pyspark.sql import SparkSession, functions as func

spark = (SparkSession
         .builder
         .master("local")
         .appName("vac_project")
         .config("spark.driver.extraClassPath", "/home/jovyan/work/jars/postgresql-9.4.1207.jar")
         .getOrCreate())

sc = spark.sparkContext

df_prices = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres/database")
    .option("dbtable", "public.prices")
    .option("user", "root")
    .option("password", "root")
    .load()
)

df = df_prices.groupby(['location', 'bedrooms']).agg(func.mean('price').alias('avg_price'))
df = df.withColumn('avg_price', func.round('avg_price',2))
df.show()