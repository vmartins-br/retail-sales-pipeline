import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

#Conectando ao banco de dados

conn = psycopg2.connect(
        dbname = "retail_sales",
        user = "vmart",
        password = "",
        host ="localhost"
)
cur = conn.cursor()

#Cria a tabela para os dados limpos

cur.execute("""
            CREATE TABLE IF NOT EXISTS cleaned_sales 
            (
            transaction_id INT PRIMARY KEY
            customer_id INT,
            category TEXT,
            item TEXT,
            price_per_unit FLOAT,
            total_sale FLOAT
            )        
            """)

#Inserindo os dados limpos

cleaned_df = pd.read_csv("data/cleaned_sales.csv")

execute_values(cur, """
               INSERT INTO cleaned_sales (transaction_id, customer_id, category, item, price_per_unit, total_sale)
               VALUES %s
               """, cleaned_df[["Transaction ID", "Customer_ID","Category","Item","Price Per Unit","Total Sale"]].values)

#Criando tabela para agregações

cur.execute("""
            CREATE TABLE IF NOT EXISTS sales_by_category
            (
            category TEXT PRIMARY KEY,
            total_sales FLOAT,
            transaction_count INT
            )
            """)

#Inserir agregações

sales_by_category_df = pd.read.csv("output/sales_by_category.csv")

execute_values(cur, """
               INSERT INTO sales_by_category (category, total_sales, transaction_count)
               VALUES %s
               """, sales_by_category_df[["Category","Total Sales","Transaction Count"]].values)

#Commit e fechar conexão

conn.commit()
cur.close()
conn.close()