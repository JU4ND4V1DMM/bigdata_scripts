from pyspark.sql import SparkSession
import os
import shutil
import string

def get_disk_with_most_free_space():
    best_drive = None
    max_free = 0
    for drive_letter in string.ascii_uppercase:
        drive = f"{drive_letter}:/"
        if os.path.exists(drive):
            try:
                total, used, free = shutil.disk_usage(drive)
                if free > max_free:
                    max_free = free
                    best_drive = drive
            except PermissionError:
                continue
    return best_drive

def try_create_spark_session(config_level):
    try:
        if config_level == "advanced":
            spark = SparkSession.builder \
                .appName("GlobalSparkApp") \
                .config("spark.local.dir", "C:/tmp/hive") \
                .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
                .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
                .config("spark.driver.memory", '16g') \
                .config("spark.executor.memory", '16g') \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
                .getOrCreate()
            spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            print("✔ Spark avanzado inicializado correctamente.")
            return spark

        elif config_level == "medium":
            spark = SparkSession.builder \
                .appName("GlobalSparkApp") \
                .config("spark.driver.memory", '8g') \
                .config("spark.executor.memory", '8g') \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
                .getOrCreate()
            spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            print("✔ Spark medio inicializado correctamente.")
            return spark

        elif config_level == "basic":
            spark = SparkSession.builder \
                .appName("GlobalSparkApp") \
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
                .getOrCreate()
            print("✔ Spark básico inicializado correctamente.")
            return spark

    except Exception as e:
        print(f"⚠ Falló configuración {config_level}: {e}")
        return None

def get_spark_session():
    configs = ["advanced", "medium", "basic"]
    for config in configs:
        spark = try_create_spark_session(config)
        if spark is not None:
            return spark
    raise RuntimeError("❌ No fue posible inicializar ninguna sesión de Spark.")