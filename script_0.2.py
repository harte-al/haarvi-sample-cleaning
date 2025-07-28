import zipfile
import pandas as pd
from pathlib import Path
from datetime import date


# Zip file path 
download_folder = Path.home() / "Downloads"
zip_files = list(download_folder.glob("*.zip"))

# File paths
log_file_path = Path.home() / "Downloads" / "invalid_data_log.txt"
master_df_path = Path.home() / "Downloads" / "all_haarvi_serum.csv"


def log_invalid_row(file_name, master_index, source_row_index, issues):
    with open(log_file_path, "a") as log_file:
        log_file.write(f"File: '{file_name}', MasterIndex: {master_index}, SourceRowIndex: {source_row_index}\n") 
        for issue in issues:
            log_file.write(f" - {issue}\n")
        log_file.write("\n")


def extract_data(download_folder, zip_files):
    """
    Input: Path to directory containing the desired zip file and zip file. 
    Output: Converts all the excel files in zip file to a sinlge pd dataframe with original source noted.
    """
    most_recent_zip = None
    try:
        print(f"\nExtracting zip file.")
        zip_files.sort(key=lambda f: f.stat().st_atime, reverse=True)
        most_recent_zip = zip_files[0]
        print(f"Successfully extracted {most_recent_zip}")
    except Exception as e:
        print(f"Error with {most_recent_zip}: {e}")


    extracted_folder = download_folder / "extracted_excel_files"
    print(f"Extracted folder: {extracted_folder}")
    extracted_folder.mkdir(exist_ok= True)

    with zipfile.ZipFile(most_recent_zip, "r") as zip_ref:
        zip_ref.extractall(extracted_folder)

    master_raw_df = []

    for file in extracted_folder.rglob(("*.xlsx")):
        try:
            print(f"\nExtracting from {file.name}")

            df = pd.read_excel(file)
            df["SourceFile"] = file.name
            df["OriginalRow"] = df.index + 2 # +2 to account for header row and 0-base index
            master_raw_df.append(df)

            print(f"Successfully read {file.name}, shape: {df.shape}")
        except Exception as e:
            print(f"Error with {file}: {e}")

    combined_df = pd.concat(master_raw_df, ignore_index=True)

    return combined_df


def validate_barcodeid(df, log_file):
    """
    Input: Dataframe from extract_data and error log file.
    Output: Validates the that the barcodeid in the df is accurate. 

    """
    # Validate invidual rows
    for idx, row in df["Barcode_ID"].items():
        issues = []
        try:
            if len(str(row)) != 8:
                issues.append(f"Barcode_ID '{row}' length is not equal to 8")
            
            elif pd.isnull(row):
                issues.append("Barcode_ID is NA")

        except Exception as e:
            print(f"Exception during 'Barcode_ID' validation: {e}")
    
    if issues:
        file_name = df.at[idx, "SourceFile"]
        source_row_index = df.at[idx, "OriginalRow"]
        log_invalid_row(file_name, idx, source_row_index, issues)

    # Check for dups
    if not df["Barcode_ID"].is_unique:
        duplicates = df[df.duplicated("Barcode_ID", keep = False)]
        grouped = duplicates.groupby("Barcode_ID").groups

    for barcode_value, indicies in grouped.items():
        issue = f"Duplicate Barcode_ID: '{barcode_value}' found in rows: {list(indicies)}"

        for idx in indicies:
            file_name = df.at[idx, "SourceFile"]
            source_row_index = df.at[idx, "OriginalRow"]
            log_invalid_row(file_name, idx, source_row_index, [issue])

    print(f"\nSuccessfully validated 'Barcode_ID', {log_file} has been updated")
    return log_file


def validate_ptid(df, log_file):
    """
    Input: Dataframe from extract_data and error log file.
    Output: Validates the that the ptid in the df is accurate.
    """
    for idx, row in df['Patient_Study_ID'].items():
        issues = []
        identifiers = ["C", "H", "IN"]
        
        try:
            if pd.isnull(row):
                issues.append(f"PTID is NA")
        
            elif not any(identifier in str(row) for identifier in identifiers):
                issues.append(f"PTID '{row}' is not correctly formatted")

        except Exception as e:
            print(f"Exception during PTID validation at index: {idx}, PTID: {row}, Exception: {e}")

        if issues:
            file_name = df.at[idx, "SourceFile"]
            source_row_index = df.at[idx, "OriginalRow"]
            log_invalid_row(file_name, idx, source_row_index, issues)
    
    print(f"\nSuccessfully validated 'PTID', {log_file} has been updated")
    return log_file


def validate_collection_date(df, log_file):
    """
    Input: Dataframe from extract_data and error log file.
    Output: Validates the that the collection date in the df is accurate and returns df with datetime formatting.
    """
    df['Date_Collected']= pd.to_datetime(df['Date_Collected'],errors='coerce', format= "%m/%d/%Y")

    start_date = date(2020, 2, 14)
    today = date.today()

    for idx, row in df['Date_Collected'].items():

        issues = []

        try:
            if pd.isnull(row):
                issues.append("Date_Collected is NA")
            
            elif row < pd.Timestamp(start_date) or row > pd.Timestamp(today):
                issues.append(f"Date_Collected is outside of range, date: {row}")
            
        except Exception as e: 
            print(f"Exception during 'Date_Collected' validation at index: {idx}, Date: {row}, Exception: {e}")

        if issues: 
            file_name = df.at[idx, "SourceFile"]
            source_row_index = df.at[idx, "OriginalRow"]
            log_invalid_row(file_name, idx, source_row_index, issues)

    print(f"\nSuccessfully validated 'Date_Collected', {log_file} has been updated")
    return log_file, df


def validate_available(df, log_file):
    """
    Input: Dataframe from extract_data and error log file.
    Output: Validates the that the availability in the df is accurate and returns df as a Boolean.
    """
    for idx, row in df['Available'].items():
        issues =[]

        try:
            if pd.isnull(row):
                df.at[idx, "Available"] = True

            elif row == "N":
                df.at[idx, "Available"] = False
            
            elif row == "Y":
                df.at[idx, "Available"] = True

            else:
                df.at[idx, "Available"] = True
                issues.append(f'Availability is unknown at index: {idx}, check to confirm availability')
        
        except Exception as e: 
            print(f"Exception during 'Available' validation at index: {idx}, Row: {row}, Exception: {e}")
    
    if issues:
        file_name = df.at[idx, "SourceFile"]
        source_row_index = df.at[idx, "OriginalRow"]
        log_invalid_row(file_name, idx, source_row_index, issues)
    
    print(f"\nSuccessfully validated 'Available', {log_file} has been updated")
    return log_file, df


# ---Processing---
 
master_raw_df = extract_data(download_folder=download_folder, zip_files=zip_files)

validate_barcodeid(df = master_raw_df, log_file=log_file_path)
validate_ptid(df=master_raw_df, log_file=log_file_path)

log_file, master_raw_df = validate_collection_date(df=master_raw_df, log_file=log_file_path)
master_raw_df = pd.DataFrame(master_raw_df)

log_file, master_raw_df =validate_available(df=master_raw_df, log_file=log_file_path)

master_raw_df.to_csv(master_df_path, index=False)