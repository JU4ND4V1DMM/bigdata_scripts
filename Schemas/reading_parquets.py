import polars as pl
from pathlib import Path

carpeta = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake\BD\Multimarca"

# Versión concisa
for archivo in Path(carpeta).glob("*.parquet"):
    print(f"\n{'='*50}")
    print(f"Archivo: {archivo.name}")
    print(f"{'='*50}")
    
    try:
        df = pl.read_parquet(archivo)
        print("Columnas:", df.columns)
        print("\nPrimer registro:")
        print(df.head(5))
        print("\nEsquema:")
        print(df.schema)
    except Exception as e:
        print(f"Error: {e}")