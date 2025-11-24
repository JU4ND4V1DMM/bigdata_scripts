import os
import polars as pl
from pyspark.sql import SparkSession

def fix_column_19_with_polars(input_folder, output_folder):
    """
    Lee archivos Parquet con Polars y renombra la columna 19 por 'segmento_1'
    """
    
    # Listar todos los archivos Parquet en la carpeta
    parquet_files = [f for f in os.listdir(input_folder) if f.endswith('.parquet')]
    
    print(f"📁 Encontrados {len(parquet_files)} archivos Parquet en: {input_folder}")
    print("=" * 80)
    
    for file_name in parquet_files:
        input_path = os.path.join(input_folder, file_name)
        output_path = os.path.join(output_folder, file_name)
        
        print(f"\n📖 Procesando: {file_name}")
        print("-" * 50)
        
        try:
            # Leer el archivo Parquet con Polars
            df = pl.read_parquet(input_path)
            
            # Obtener los nombres de columnas originales
            original_columns = df.columns
            
            print(f"🔹 Columnas originales ({len(original_columns)}):")
            for i, col in enumerate(original_columns):
                print(f"    {i+1:2d}. {col}")
            
            # Verificar si existe la columna 19 (índice 18)
            if len(original_columns) >= 19:
                columna_19_original = original_columns[18]
                print(f"\n🔄 Reemplazando columna 19: '{columna_19_original}' → 'segmento_1'")
                
                # Renombrar solo la columna 19 usando Polars
                df = df.rename({columna_19_original: "segmento_1"})
                
                # Mostrar columnas finales
                final_columns = df.columns
                print(f"\n✅ Columnas finales ({len(final_columns)}):")
                for i, col in enumerate(final_columns):
                    print(f"    {i+1:2d}. {col}")
                
                # Guardar el archivo corregido con Polars
                df.write_parquet(output_path)
                print(f"\n💾 Archivo guardado en: {output_path}")
                print(f"🎯 Columna 19 renombrada exitosamente: '{columna_19_original}' → 'segmento_1'")
            else:
                print(f"⚠️  El archivo tiene solo {len(original_columns)} columnas, no se puede acceder a la columna 19")
                # Guardar el archivo sin cambios
                df.write_parquet(output_path)
                print(f"💾 Archivo guardado sin cambios: {output_path}")
                
        except Exception as e:
            print(f"❌ Error procesando {file_name}: {str(e)}")
            # Intentar método alternativo para archivos corruptos
            try:
                print("🔄 Intentando método alternativo con scan_parquet...")
                df = pl.scan_parquet(input_path).collect()
                
                if len(df.columns) >= 19:
                    columna_19_original = df.columns[18]
                    df = df.rename({columna_19_original: "segmento_1"})
                    df.write_parquet(output_path)
                    print(f"💾 Archivo guardado con método alternativo: {output_path}")
                else:
                    df.write_parquet(output_path)
                    print(f"💾 Archivo guardado sin cambios (método alternativo): {output_path}")
                    
            except Exception as e2:
                print(f"❌ Método alternativo también falló: {str(e2)}")
        
        print("-" * 50)

# Configurar paths
input_folder = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake\Base General Mensual"
output_folder = r"C:\Users\juan_\Downloads\New folder"

# Crear carpeta de salida si no existe
os.makedirs(output_folder, exist_ok=True)

# Instalar Polars si no lo tienes: pip install polars
try:
    fix_column_19_with_polars(input_folder, output_folder)
    print("\n🎉 Proceso completado exitosamente!")
except Exception as e:
    print(f"❌ Error general: {str(e)}")