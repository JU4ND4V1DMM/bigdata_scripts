import polars as pl
from pathlib import Path
from datetime import datetime
import os

def convert_csv_to_parquet_in_place(input_folder: str) -> None:
    """
    Minimalist CSV to Parquet converter that replaces CSV files in the same folder.
    Converts all CSV files in the specified folder and its subfolders.
    """
    separator = ";"
    input_path = Path(input_folder)
    
    print(f"🔄 Converting CSV to Parquet in place: {input_folder}")
    
    # Counter for statistics
    total_converted = 0
    total_failed = 0
    
    # Process all CSV files recursively
    for file_path in input_path.rglob('*.csv'):
        try:
            # Skip files that are already in process or temporary
            if file_path.name.startswith('~') or file_path.name.startswith('.'):
                continue
                
            # Output file will be in the same location as the CSV
            output_file = file_path.with_suffix('.parquet')
            
            # Try different encodings
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pl.read_csv(
                        file_path, 
                        separator=separator,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        infer_schema_length=100000,
                        encoding=encoding
                    )
                    print(f"📖 {file_path.name} read with {encoding} encoding")
                    break
                except Exception as e:
                    continue
            
            if df is None:
                print(f"❌ {file_path.name}: Could not read with any encoding")
                total_failed += 1
                continue
            
            # Write Parquet file
            df.write_parquet(output_file)
            print(f"✅ {file_path.name} -> {output_file.name} ({len(df):,} rows, {len(df.columns)} cols)")
            
            # Verify Parquet file was created successfully
            if output_file.exists():
                # Delete the original CSV file
                file_path.unlink()
                print(f"🗑️  Deleted: {file_path.name}")
                total_converted += 1
            else:
                print(f"❌ {file_path.name}: Parquet file not created, keeping CSV")
                total_failed += 1
                
        except Exception as e:
            print(f"❌ {file_path.name}: {e}")
            total_failed += 1
    
    # Print summary
    print(f"\n🎉 Conversion completed!")
    print(f"📊 Summary: {total_converted} files converted, {total_failed} files failed")

# # Usage for your specific folder
if __name__ == "__main__":
    target_folder = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake\BD\Castigo"
    convert_csv_to_parquet_in_place(target_folder)