from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.appName("RetailSalesCleaning").getOrCreate()

df = spark.read.csv("C://Users//vmart//OneDrive//Documentos//ETL//retail-sales-pipeline//data//retail_store_sales.csv", header=True, inferSchema=True)

df.show(10);