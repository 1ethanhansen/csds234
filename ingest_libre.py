# Source for data: https://www.kaggle.com/datasets/anonymousfrog95/cgm-data

import csv
import sqlite3
import os
from datetime import datetime
from pathlib import Path

def process_csv_file(file_path, series_id, conn):
    """Process CSV file and insert data into SQLite database."""
    # Get just the filename without the path
    file_name = os.path.basename(file_path)

    # Insert into series table and get the series_id
    cursor = conn.cursor()
    cursor.execute("INSERT INTO file (file_name, series_id) VALUES (?, ?)", (file_name, series_id))
    conn.commit()
    
    with open(file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)

        header = None
        
        for row in csv_reader:
            if not row:  # Skip empty rows
                continue
            # Detect header row
            if header is None and 'Time' in row:
                header = row
                continue
            
            if header: 
                process_cgm_row(row, header, series_id, conn)

def process_cgm_row(row, header, series_id, conn):
    """Process a row from libre_cgm_dataset.csv."""
    # Map columns to their indices
    try:
        # Extract indices for key fields
        date_idx = header.index("Date")
        time_idx = header.index("Time")
        glucose_idx = header.index("Glucose mmol/L")
        
        # Extract data
        date_str = row[date_idx] if date_idx < len(row) else ""
        time_str = row[time_idx] if time_idx < len(row) else ""
        glucose_lvl = row[glucose_idx] if glucose_idx < len(row) else ""
        
        if not date_str or not time_str or not glucose_lvl:
            return
            
        # Rearrange date string to match formatting
        parts = date_str.split("/")
        rearranged_date = f"{parts[2]}/{parts[0]}/{parts[1]}"

        # Combine date and time into datetime
        date_str = rearranged_date.replace('/', '-')
        datetime_str = f"{date_str}T{time_str}"
        
        # Insert glucose data
        glucose_lvl = float(glucose_lvl) * 18 # Converts mmol/L to mg/dL
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO cgm_data (datetime, series_id, blood_glucose) VALUES (?, ?, ?)",
            (datetime_str, series_id, glucose_lvl)
        )
        conn.commit()
            
    except (ValueError,IndexError):
        # If any column is not found, just skip this row
        pass

def main():
    # Configuration
    db_name = "cgm.db"
    csv_file_path = "./input_data/libre_cgm_dataset"
    
    # Connect to database
    conn = sqlite3.connect(db_name)

    # Insert into series table and get the series_id
    cursor = conn.cursor()
    cursor.execute("INSERT INTO series DEFAULT VALUES")
    series_id = cursor.lastrowid
    conn.commit()
    
    # Process all CSV files in the directory
    for file in Path(csv_file_path).glob("*.csv"):
        process_csv_file(str(file), series_id, conn)
    
    print(f"Data from {csv_file_path} has been imported into {db_name}")
    conn.close()

if __name__ == "__main__":
    main()