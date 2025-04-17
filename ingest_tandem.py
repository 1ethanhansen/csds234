import csv
import sqlite3
import os
import datetime
from pathlib import Path

def create_database(db_name):
    """Create SQLite database with the specified schema."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    # Create tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS series (
        series_id INTEGER PRIMARY KEY
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS file (
        id INTEGER PRIMARY KEY,
        file_name TEXT,
        series_id INTEGER,
        FOREIGN KEY (series_id) REFERENCES series (series_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS food_data (
        id INTEGER PRIMARY KEY,
        datetime TEXT,
        series_id INTEGER,
        carb_count REAL,
        FOREIGN KEY (series_id) REFERENCES series (series_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cgm_data (
        id INTEGER PRIMARY KEY,
        datetime TEXT,
        series_id INTEGER,
        blood_glucose REAL,
        FOREIGN KEY (series_id) REFERENCES series (series_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bolus_data (
        id INTEGER PRIMARY KEY,
        datetime TEXT,
        series_id INTEGER,
        bolus_amt REAL,
        FOREIGN KEY (series_id) REFERENCES series (series_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS basal_data (
        id INTEGER PRIMARY KEY,
        datetime TEXT,
        series_id INTEGER,
        basal_amt REAL,
        FOREIGN KEY (series_id) REFERENCES series (series_id)
    )
    ''')
    
    conn.commit()
    return conn

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

        current_format = ""
        header = ""
        
        for row in csv_reader:
            if not row:  # Skip empty rows
                continue
            
            # Try to detect if this is a new header row
            if any(keyword in ' '.join(row) for keyword in ["DeviceType", "Type", "BolusType"]):
                header = row
                if "DeviceType" in row and "SerialNumber" in row and "Readings (mg/dL)" in row:
                    current_format = "cgm"
                elif "Type" in row and "BolusType" in row:
                    current_format = "treatment"
                continue
            
            if current_format == "cgm":
                process_cgm_row(row, header, series_id, conn)
            elif current_format == "treatment":
                process_treatment_row(row, header, series_id, conn)

def process_cgm_row(row, header, series_id, conn):
    """Process a row from the CGM format."""
    # Map columns to their indices
    try:
        datetime_idx = header.index("EventDateTime")
        readings_idx = header.index("Readings (mg/dL)")
        
        # Extract data
        event_datetime = row[datetime_idx] if datetime_idx < len(row) else ""
        
        if not event_datetime:
            return
        
        # Insert into appropriate table based on device_type and description
        if readings_idx < len(row) and row[readings_idx]:
            try:
                blood_glucose = float(row[readings_idx])
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO cgm_data (datetime, series_id, blood_glucose) VALUES (?, ?, ?)",
                    (event_datetime, series_id, blood_glucose)
                )
                conn.commit()
            except (ValueError, IndexError):
                pass
    except ValueError:
        # If any column is not found, just skip this row
        pass

def process_treatment_row(row, header, series_id, conn):
    """Process a row from the treatment format."""
    try:
        # Extract indices for key fields
        type_idx = header.index("Type") if "Type" in header else -1
        bolus_type_idx = header.index("BolusType") if "BolusType" in header else -1
        bg_idx = header.index("BG (mg/dL)") if "BG (mg/dL)" in header else -1
        completion_datetime_idx = header.index("CompletionDateTime") if "CompletionDateTime" in header else -1
        insulin_delivered_idx = header.index("InsulinDelivered") if "InsulinDelivered" in header else -1
        food_delivered_idx = header.index("FoodDelivered") if "FoodDelivered" in header else -1
        carb_size_idx = header.index("CarbSize") if "CarbSize" in header else -1
        
        if type_idx < 0 or completion_datetime_idx < 0:
            return
            
        row_type = row[type_idx] if type_idx < len(row) else ""
        completion_datetime = row[completion_datetime_idx] if completion_datetime_idx < len(row) else ""
        
        if not completion_datetime:
            return
            
        # Process bolus data
        if row_type == "Bolus" and insulin_delivered_idx >= 0 and insulin_delivered_idx < len(row):
            try:
                insulin_delivered = float(row[insulin_delivered_idx]) if row[insulin_delivered_idx] else 0
                if insulin_delivered > 0:
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO bolus_data (datetime, series_id, bolus_amt) VALUES (?, ?, ?)",
                        (completion_datetime, series_id, insulin_delivered)
                    )
                    conn.commit()
            except (ValueError, IndexError):
                pass
                
        # Alternative carb data from CarbSize field
        if carb_size_idx >= 0 and carb_size_idx < len(row):
            try:
                carb_size = float(row[carb_size_idx]) if row[carb_size_idx] else 0
                # print(carb_size)
                if carb_size > 0:
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO food_data (datetime, series_id, carb_count) VALUES (?, ?, ?)",
                        (completion_datetime, series_id, carb_size)
                    )
                    conn.commit()
            except (ValueError, IndexError):
                pass
                
        # Process basal data (if available)
        if row_type == "Basal" and insulin_delivered_idx >= 0 and insulin_delivered_idx < len(row):
            try:
                basal_amt = float(row[insulin_delivered_idx]) if row[insulin_delivered_idx] else 0
                if basal_amt > 0:
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO basal_data (datetime, series_id, basal_amt) VALUES (?, ?, ?)",
                        (completion_datetime, series_id, basal_amt)
                    )
                    conn.commit()
            except (ValueError, IndexError):
                pass
                
    except ValueError:
        # If any column is not found, just skip this row
        pass

def main():
    # Configuration
    db_name = "cgm.db"
    csv_directory = "./input_data/personal_data"
    
    # Create or connect to database
    conn = create_database(db_name)

    # Insert into series table and get the series_id
    cursor = conn.cursor()
    cursor.execute("INSERT INTO series DEFAULT VALUES")
    series_id = cursor.lastrowid
    conn.commit()
    
    # Process all CSV files in the directory
    csv_files = list(Path(csv_directory).glob("CSV_*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {csv_directory}")
        return
        
    for csv_file in csv_files:
        print(f"Processing {csv_file}...")
        process_csv_file(str(csv_file), series_id, conn)
        
    print(f"Data has been imported into {db_name}")
    conn.close()

if __name__ == "__main__":
    main()