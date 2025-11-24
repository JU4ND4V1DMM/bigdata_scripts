import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit

spark = SparkSession.builder \
    .appName("Merge CSV Files") \
    .getOrCreate()

def read_csv_with_delimiter(file_path: str, delimiter: str) -> DataFrame:
    return spark.read.csv(file_path, sep=delimiter, header=True, inferSchema=True)

directory_path = "C:/Users/c.operativo/Downloads/Nueva carpeta/6. Junio"
file_paths = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]
merged_df = None

for file_path in file_paths:
    delimiter = None
    with open(file_path, 'r') as f:
        first_line = f.readline()
        if ',' in first_line:
            delimiter = ','
        elif ';' in first_line:
            delimiter = ';'
    
    if delimiter is not None:
        try:
            df = read_csv_with_delimiter(file_path, delimiter)
        except Exception as e:
            continue  
        
        if merged_df is None:
            merged_df = df
        else:
            merged_df = merged_df.unionByName(df, allowMissingColumns=True)

if merged_df is not None:
    
    output_path = "C:/Users/c.operativo/Downloads/ResultadoDemos"
    
    merged_df = merged_df.withColumn("Cuenta_Real", concat(col("Cuenta_Real"), lit("-")))
    merged_df = merged_df.withColumn("Cuenta_Sin_Punto", concat(col("Cuenta_Sin_Punto"), lit("-")))
    merged_df = merged_df.withColumn("MES DE ASIGNACION", lit(6))
    merged_df = merged_df.withColumn("PERIODO DE ASIGNACION", lit(2024))
    
    merged_df = merged_df.dropDuplicates()
    merged_df.repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(output_path)
    
else:
    print("No data was merged.")