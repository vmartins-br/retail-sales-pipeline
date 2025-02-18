from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.appName("RetailSalesCleaning").getOrCreate()

df = spark.read.csv("data/retail_store_sales.csv", header=True, inferSchema=True)

#Removendo duplicatas

df = df.dropDuplicates()

#Tratando valores faltantes

df = df.na.drop(subset=["Transaction ID", "Customer ID","Price Per Unit"])
df = df.na.fill({"Category": "Unknown", "Item": "Unknown"})

#Corrigindo tipos de dados

df = df.withColumn("Price Per Unit", col("Price Per Unit").cast("float"))

#Padronizando dados

df = df.withColumn("Category", trim(col("Category")))
df = df.withColumn("Item", trim(col("Item")))

#Validando dados

df = df.filter(col("Price Per Unit") >= 0)

#Salvando os dados limpos

df.write.mode("overwrite").csv("data/cleaned_sales", header=True)

#Exibindo Schema e primeiros registros

df.printSchema()
df.show(10)