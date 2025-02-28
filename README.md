# Retail Sales Pipeline

Este projeto é um pipeline de dados para limpeza, processamento e análise de dados de vendas de uma loja de varejo. Ele utiliza PySpark para processamento distribuído, SQL Server para armazenamento de dados e Python para análise e visualização.

---

## Objetivo

O objetivo deste projeto é:
1. Realizar a **limpeza** e **transformação** de dados sujos de vendas.
2. Processar os dados usando **PySpark**.
3. Armazenar os dados limpos em um banco de dados **SQL Server**.
4. Realizar análises e gerar visualizações para insights de negócios.

---

## Tecnologias Utilizadas

- **Python**: Para scripts de limpeza, transformação e análise.
- **PySpark**: Para processamento distribuído de grandes volumes de dados.
- **SQL Server**: Para armazenamento dos dados limpos e agregações.
- **Matplotlib/Seaborn**: Para visualização de dados.
- **Jupyter Notebook**: Para análise interativa e visualização.

---

## Estrutura do Projeto
retail-sales-pipeline/
  │
  ├── data/
  │ ├── retail_store_sales_dirty.csv # Dataset original (dados sujos)
  │ ├── cleaned_sales/ # Dados limpos (gerados pelo script)
  │ └── sales_by_category/ # Agregações por categoria (geradas pelo script)
  │
  ├── notebooks/
  │ └── analysis.ipynb # Notebook para análise e visualização
  │
  ├── scripts/
  │ ├── data_cleaning.py # Script para limpeza dos dados
  │ ├── data_processing.py # Script para transformações e agregações
  │ └── data_loading.py # Script para carregar dados no SQL Server  
  │
  ├── README.md # Documentação do projeto
  └── requirements.txt # Dependências do projeto
