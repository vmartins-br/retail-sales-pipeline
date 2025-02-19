from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
import pandas as pd

#Realizando transformações e agregações nos dados limpos

spark = SparkSession.builder.config("spark.driver.host","localhost").appName("RetailSalesProcessing").getOrCreate()

#Carregando dados limpos

cleaned_df = spark.read.csv("data/cleaned_sales.csv", header=True, inferSchema=True)

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

#Convertendo DF para Pandas

sales_by_category_pd = sales_by_category.toPandas()

sales_by_customer_pd = sales_by_customer.toPandas()

#Salvando os arquivos em tabelas

try:

    sales_by_category_pd.to_csv("data/sales_by_category.csv", index=False)
    print("Arquivo salvo com sucesso!")

except:
    print("Erro! Permissão negada!")


try:

    sales_by_customer_pd.to_csv("data/sales_by_costumer.csv", index=False)
    print("Arquivo salvo com sucesso!")    

except:
    print("Erro! Permissão negada!")

#Exibindo resultados

print("Total de Vendas por Categoria:")
sales_by_category.show(30)

print("Total de Vendas por Cliente:")
sales_by_customer.show(30)

spark.stop()