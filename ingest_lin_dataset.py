import csv
import sqlite3
import os
from pathlib import Path

def process_csv_file(file_path, series_id, conn):
    """Process CSV file and insert glucose data into SQLite database."""
    file_name = os.path.basename(file_path)
    
    # Insert into file table
    cursor = conn.cursor()
    cursor.execute("INSERT INTO file (file_name, series_id) VALUES (?, ?)", (file_name, series_id))
    conn.commit()
    
    with open(file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = None
        
        for row in csv_reader:
            if not row:  # Skip empty rows
                continue
                
            # Find the header row
            if "Device" in row[0] and "Serial Number" in row[1] and "Device Timestamp" in row[2]:
                header = row
                continue
                
            if header:
                process_glucose_row(row, header, series_id, conn)

def process_glucose_row(row, header, series_id, conn):
    """Process a row to extract glucose data."""
    try:
        # Get column indices
        timestamp_idx = header.index("Device Timestamp")
        glucose_idx = header.index("Historic Glucose mmol/L")
        
        # Extract timestamp and glucose value
        timestamp = row[timestamp_idx] if timestamp_idx < len(row) else ""
        glucose_value = row[glucose_idx] if glucose_idx < len(row) else ""
        
        if not timestamp or not glucose_value:
            return
            
        # Convert date format from "d/m/YYYY H:MM" to ISO format "YYYY-MM-DDTHH:MM"
        day, month, year_time = timestamp.split('/')
        year, time = year_time.split(' ')
        iso_timestamp = f"20{year}-{month.zfill(2)}-{day.zfill(2)}T{time}"
        
        # Convert glucose from mmol/L to mg/dL
        try:
            glucose_mgdl = float(glucose_value) * 18.0182
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO cgm_data (datetime, series_id, blood_glucose) VALUES (?, ?, ?)",
                (iso_timestamp, series_id, glucose_mgdl)
            )
            conn.commit()
        except ValueError:
            pass
                
    except ValueError:
        # If any column is not found, just skip this row
        pass

def main():
    # Configuration
    db_name = "cgm.db"
    csv_directory = "./input_data/lin_dataset"
    
    # Create or connect to database
    conn = create_database(db_name)

    # Insert into series table and get the series_id
    cursor = conn.cursor()
    cursor.execute("INSERT INTO series DEFAULT VALUES")
    series_id = cursor.lastrowid
    conn.commit()
    
    # Process all CSV files in the directory
    csv_files = list(Path(csv_directory).glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {csv_directory}")
        return
        
    for csv_file in csv_files:
        print(f"Processing {csv_file}...")
        process_csv_file(str(csv_file), series_id, conn)
        
    print(f"Glucose data has been imported into {db_name}")
    conn.close()

if __name__ == "__main__":
    main()