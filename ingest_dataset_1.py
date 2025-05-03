# Source for data: https://www.kaggle.com/datasets/diabetes1123/diabetes-type-1-dataset?resource=download

import csv
import sqlite3
import os
import datetime
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

        current_format = ""
        header = ""
        
        for row in csv_reader:
            if not row:  # Skip empty rows
                continue

            # print(row)
            
            # Try to detect if this is a new header row
            if any(keyword in ' '.join(row) for keyword in ["date", "time", "basal_rate", "bolus_volume_delivered", "glucose_level", "meal_kcal"]):
                header = row
                if "basal_rate" in row:
                    current_format = "basal"
                elif "bolus_volume_delivered" in row:
                    current_format = "bolus"
                elif "glucose_level" in row:
                    current_format = "cgm"
                elif "meal_kcal" in row:
                    current_format = "meal"
                continue
            
            if current_format == "basal":
                process_basal_row(row, header, series_id, conn)
            elif current_format == "bolus":
                process_bolus_row(row, header, series_id, conn)
            elif current_format == "cgm":
                process_cgm_row(row, header, series_id, conn)
            elif current_format == "meal":
                process_meal_row(row, header, series_id, conn)

def process_basal_row(row, header, series_id, conn):
    """Process a row from basals.csv."""
    try:
        # Extract indices for key fields
        date_idx = header.index("date")
        time_idx = header.index("time")
        basal_rate_idx = header.index("basal_rate")
        
        # Extract data
        date_str = row[date_idx] if date_idx < len(row) else ""
        time_str = row[time_idx] if time_idx < len(row) else ""
        basal_rate = row[basal_rate_idx] if basal_rate_idx < len(row) else ""
        
        if not date_str or not time_str or not basal_rate:
            return
            
        # Combine date and time into datetime
        date_str = date_str.replace('/', '-')
        datetime_str = f"{date_str}T{time_str}"
        
        # Insert basal data
        try:
            basal_amt = float(basal_rate)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO basal_data (datetime, series_id, basal_amt) VALUES (?, ?, ?)",
                (datetime_str, series_id, basal_amt)
            )
            conn.commit()
        except (ValueError, IndexError):
            pass
            
    except ValueError:
        # If any column is not found, just skip this row
        pass

def process_cgm_row(row, header, series_id, conn):
    """Process a row from glucose.csv."""
    # Map columns to their indices
    try:
        # Extract indices for key fields
        date_idx = header.index("date")
        time_idx = header.index("time")
        glucose_idx = header.index("glucose_level")
        
        # Extract data
        date_str = row[date_idx] if date_idx < len(row) else ""
        time_str = row[time_idx] if time_idx < len(row) else ""
        glucose_lvl = row[glucose_idx] if glucose_idx < len(row) else ""
        
        if not date_str or not time_str or not glucose_lvl:
            return
            
        # Combine date and time into datetime
        date_str = date_str.replace('/', '-')
        datetime_str = f"{date_str}T{time_str}"
        
        # Insert basal data
        try:
            glucose_lvl = float(glucose_lvl)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO cgm_data (datetime, series_id, blood_glucose) VALUES (?, ?, ?)",
                (datetime_str, series_id, glucose_lvl)
            )
            conn.commit()
        except (ValueError, IndexError):
            pass
            
    except ValueError:
        # If any column is not found, just skip this row
        pass

def process_bolus_row(row, header, series_id, conn):
    """Process a row from boluses.csv."""
    try:
        # Extract indices for key fields
        date_idx = header.index("date")
        time_idx = header.index("time")
        bolus_idx = header.index("bolus_volume_delivered")
        
        # Extract data
        date_str = row[date_idx] if date_idx < len(row) else ""
        time_str = row[time_idx] if time_idx < len(row) else ""
        unit_count = row[bolus_idx] if bolus_idx < len(row) else ""
        
        if not date_str or not time_str or not unit_count:
            return
            
        # Combine date and time into datetime
        date_str = date_str.replace('/', '-')
        datetime_str = f"{date_str}T{time_str}"
        
        # Insert basal data
        try:
            unit_count = float(unit_count)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO bolus_data (datetime, series_id, bolus_amt) VALUES (?, ?, ?)",
                (datetime_str, series_id, unit_count)
            )
            conn.commit()
        except (ValueError, IndexError):
            pass
            
    except ValueError:
        # If any column is not found, just skip this row
        pass

def process_meal_row(row, header, series_id, conn):
    """Process a row from boluses.csv."""
    try:
        # Extract indices for key fields
        date_idx = header.index("date")
        time_idx = header.index("time")
        meal_kcal_idx = header.index("meal_kcal")
        
        # Extract data
        date_str = row[date_idx] if date_idx < len(row) else ""
        time_str = row[time_idx] if time_idx < len(row) else ""
        meal_kcal = row[meal_kcal_idx] if meal_kcal_idx < len(row) else ""
        
        if not date_str or not time_str or not meal_kcal:
            return
            
        # Combine date and time into datetime
        date_str = date_str.replace('/', '-')
        datetime_str = f"{date_str}T{time_str}"
        
        # Insert basal data
        try:
            carb_count = float(meal_kcal) / 8 # we have to estimate kcal->grams carbs
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO food_data (datetime, series_id, carb_count) VALUES (?, ?, ?)",
                (datetime_str, series_id, carb_count)
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
    csv_directory = "./input_data/dataset_1"
    
    # Create or connect to database
    conn = sqlite3.connect(db_name)

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
        
    print(f"Data from {csv_directory} has been imported into {db_name}")
    conn.close()

if __name__ == "__main__":
    main()