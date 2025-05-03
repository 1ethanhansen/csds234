import csv
import sqlite3
import os
from pathlib import Path
from datetime import datetime
from itertools import islice

def create_connection(db_name):
    """Create a connection to the SQLite database."""
    return sqlite3.connect(db_name)

def process_csv_file(file_path, series_id, conn):
    
    """Process CSV file and insert data into SQLite database."""
    # Get just the filename without the path
    file_name = os.path.basename(file_path)

    # Insert into series table and get the series_id
    cursor = conn.cursor()
    cursor.execute("INSERT INTO file (file_name, series_id) VALUES (?, ?)", (file_name, series_id))
    conn.commit()

    with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.reader(csv_file)

        header = None
        

        for row in csv_reader:
            if not row:  
                continue
            # Detect a header row
            if "Time" in row and header is None:
                header = row
                continue
            if header: 
                process_cgm_row(row, header, series_id, conn)


def process_cgm_row(row, header, series_id, conn):
    """Process a row in the CGM format."""
    # Map columns to their indices
    try:
        date_idx = header.index("Date")
        time_idx = header.index("Time")
        glucose_idx = header.index("Glucose mmol/L")

        date_str = row[date_idx].strip()
        time_str = row[time_idx].strip()
        glucose_str = row[glucose_idx].strip()
        # Extract data
        if not date_str or not time_str or not glucose_str:
            return
        
         # Convert date + time to ISO format
        dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        iso_datetime = dt.isoformat()
        # Convert mmol/L to mg/dL
        blood_glucose = round(float(glucose_str) * 18.018, 1)

        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO cgm_data (datetime, series_id, blood_glucose) VALUES (?, ?, ?)",
            (iso_datetime, series_id, blood_glucose)
        )
        conn.commit()
    except (ValueError, IndexError):
        pass


def main():
    # Configuration
    db_name = "cgm.db"
    csv_file_path = "./input_data/labelled_cgm_dataset"

    # Connect to database
    conn = create_connection(db_name)

    # Insert into series table and get the series_id
    cursor = conn.cursor()
    cursor.execute("INSERT INTO series DEFAULT VALUES")
    series_id = cursor.lastrowid
    conn.commit()

    # Process CSV file in the directory
    for file in Path(csv_file_path).glob("*.csv"):
        process_csv_file(str(file), series_id, conn)
    
    print(f"Data has been imported into {db_name}")
    conn.close()
    
if __name__ == "__main__":
    main()