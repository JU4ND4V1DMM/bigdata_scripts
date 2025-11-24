import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def upload_files_dbeaver(root_path, database):
    
    # Configuración de PostgreSQL
    DB_NAME = "Claro_Data_Overview"
    DB_USER = "postgres"
    DB_PASSWORD = "0127241976"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    JDBC_DRIVER = "org.postgresql.Driver"

    # Carpeta con los archivos CSV y Parquet
    DATA_FOLDER = root_path

    # Configuración de Spark
    RAM = "32g"
    TIMEOUT = "200"

    os.environ["PYSPARK_DRIVER_MEMORY"] = RAM
    os.environ["PYSPARK_EXECUTOR_MEMORY"] = RAM
    os.environ["PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT"] = TIMEOUT

    spark = SparkSession.builder \
        .appName("CSV_Parquet_to_Postgres") \
        .config("spark.driver.memory", RAM) \
        .config("spark.executor.memory", RAM) \
        .config("spark.sql.shuffle.partitions", TIMEOUT) \
        .config("spark.local.dir", "D:/SparkTemp") \
        .config("spark.jars", "file:///D:/Programs/PostgreSQL/17/lib/postgresql-42.7.3.jar") \
        .getOrCreate()

    def process_file(file_path, file_type, database):
        """Procesa un archivo según su tipo y lo carga a PostgreSQL"""
        print(f"Procesando archivo: {os.path.basename(file_path)}")
        
        try:
            # Leer archivo según el tipo
            if file_type == "csv":
                df = spark.read.option("header", "true").option("sep", ";").csv(file_path)
            elif file_type == "parquet":
                df = spark.read.parquet(file_path)
            else:
                print(f"Tipo de archivo no soportado: {file_type}")
                return
            
            # Normalizar nombres de columnas
            df = df.toDF(*[col_name.lower().replace(" ", "_").replace("-", "_") for col_name in df.columns])

            # Convertir todas las columnas a STRING
            for column in df.columns:
                df = df.withColumn(column, col(column).cast("STRING"))

            # Escribir en PostgreSQL
            df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", database) \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", JDBC_DRIVER) \
                .mode("append") \
                .save()
                
            print(f"✅ Archivo {os.path.basename(file_path)} procesado exitosamente")
            
        except Exception as e:
            print(f"❌ Error procesando archivo {os.path.basename(file_path)}: {str(e)}")

    # Procesar todos los archivos en la carpeta
    for file in os.listdir(DATA_FOLDER):
        file_path = os.path.join(DATA_FOLDER, file)
        
        if file.endswith(".csv"):
            process_file(file_path, "csv", database)
        elif file.endswith(".parquet"):
            process_file(file_path, "parquet", database)

    print("Carga de todos los archivos completada.")
    spark.stop()
    

database = "data"
root_path = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake\BD\Multimarca"

upload_files_dbeaver(root_path, database)