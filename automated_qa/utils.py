import pandas as pd
from datetime import datetime
import os

def read_frame(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    read_funcs = {".csv": pd.read_csv, ".xlsx": pd.read_excel, ".parquet": pd.read_parquet}
    reading_func = read_funcs.get(ext)

    if not reading_func:
        raise ValueError(f"Unsupported file format: {ext}")
    try:
        df = reading_func(path, encoding="utf-8-sig") if ext == ".csv" else reading_func(path)
    except:
        df = reading_func(path, encoding="latin") if ext == ".csv" else reading_func(path)
    return df

def display_datasets(api):
    datasets = api.get_datasets()
    if datasets:
        print("Available Dataset IDs:")
        for dataset in datasets:
            print(f"ID: {dataset['id']} - Name: {dataset.get('name', 'Unnamed')}")
    else:
        print("No datasets found.")

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        exit(f"Not a valid date: '{s}'. Expected format: YYYY-MM-DD.")

def check_critical_columns_present(frame, columns, new=True):
    frame_cols = frame.columns
    for col in columns:
        if col not in frame_cols:
            if new:
                exit(f"Critical column '{col}' not found in the new file.")
            else:
                exit(f"Critical column '{col}' not found in the old file.")