from pyspark.sql import SparkSession
import re
import os

spark = SparkSession.builder \
    .appName("GenerateSQL") \
    .config("spark.local.dir", "D:/SparkTemp") \
    .getOrCreate()
    
def format_column_name(name: str) -> str:
    return re.sub(r'\s+', '_', name.lower())

def generate_create_table_sql_pyspark(file_path: str, table_name: str = "read_data_files"):
    
    if file_path.lower().endswith('.csv'):
        df = spark.read.csv(file_path, header=True, sep=";")
    elif file_path.lower().endswith('.parquet'):
        df = spark.read.parquet(file_path)
    else:
        raise ValueError("Solo se admiten archivos CSV y Parquet")
    
    columns = [format_column_name(col) for col in df.columns]
    columns_sql = ",\n    ".join([f'"{col}" TEXT' for col in columns])

    create_table_sql = f"""
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    {columns_sql},
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
    """
    
    print(create_table_sql)
    return create_table_sql

file_path = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake\BD\Multimarca\Compilado Data Claro 2024 - 10.parquet"

try:
    generate_create_table_sql_pyspark(file_path)
finally:
    spark.stop()