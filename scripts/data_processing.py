from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

#Realizando transformações e agregações nos dados limpos

spark = SparkSession.builder.appName("RetailSalesProcessing").getOrCreate()

#Carregando dados limpos

spark = spark.read.csv("data/cleaned_sales", header=True, inferSchema=True)

#Adicionando coluna total de vendas

cleaned_df = cleaned_df.withColumn("Total Sale", col("Price Per Unit"))

#Adicionando agregações
#Total de vendas por categoria

sales_by_category = cleaned_df.groupBy("Category").agg(
    sum("Total Sale").alias("Total Sales"),
    count("Transaction ID").alias("Transaction Count"))

#Total de vendas por cliente

sales_by_customer = cleaned_df.groupBy("Customer ID").agg(
    sum("Total Sale").alias("Total Sales"),
    count("Transaction Id").alias("Transaction Count"))

#Salvando resultados

sales_by_category.write.mode("overwrite").csv("output/sales_by_category", header=True)
sales_by_customer.write.mode("overwrite").csv("output/sales_by_customer", header=True)

#Exibindo resultados

print("Total de Vendas por Categoria:")
sales_by_category.show(30)

print("Total de Vendas por Cliente:")
sales_by_customer.show(30)

spark.stop()