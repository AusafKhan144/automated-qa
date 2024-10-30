import pandas as pd
from datetime import datetime
from automated_qa.utils import read_frame,check_critical_columns_present


def get_feed_date(df):
    formats = ["%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"]
    column = next((col for col in df.columns if "DateExtract" in col), "DateExtractRun")
    for format in formats:
        try:
            return datetime.strptime(
                str(df[column].unique().tolist()[0]), format
            ).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return datetime.now().strftime("%Y-%m-%d")


def get_null_counts(df, columns, provided):
    return {
        col: (df[col].isnull().sum(), "N/A or Blank values")
        for col in columns
        if not provided or df[col].isnull().sum() != 0
    }


def add_rows_to_frame(frame, data_dict, stats_type, feed_date, dataset_id):
    rows = [
        {
            "type": stats_type,
            "value": val[0],
            "reason": val[1],
            "related_columns": col,
            "dataset_id": dataset_id,
            "feed_date": feed_date,
        }
        for col, val in data_dict.items()
    ]
    return pd.concat([frame, pd.DataFrame(rows)], ignore_index=True)

def get_null_counts(df: pd.DataFrame, columns: list[str], provided):
    null_counts = {}
    for column in columns:
        null_counts[column] = (df[column].isnull().sum(), "N/A or Blank values")
    if not provided:
        null_counts = {key: val for key, val in null_counts.items() if val[0] != 0}
    return null_counts


def check_outliers(new_df: pd.DataFrame, old_df: pd.DataFrame) -> dict:
    new_df = new_df.fillna("N/A")
    old_df = old_df.fillna("N/A")
    expected_types = old_df.dtypes.to_dict()

    type_mismatch_logs = {}

    for column, expected_type in expected_types.items():
        if column in new_df.columns:
            actual_type = new_df[column].dtype
            if actual_type != expected_type:
                type_mismatch_logs[column] = (0, "Datatype mismatch")
        else:
            type_mismatch_logs[column] = (0, "Column missing")

    return type_mismatch_logs


def get_total_count(df):
    return {"All": (len(df), "Count of all rows in the dataset")}


def get_duplicate_count(df,columns):
    return {"All": (df.duplicated(subset=columns).sum(), "Count of duplicate rows in the dataset")}


def check_price_columns_count(
    new_df: pd.DataFrame, current_price_col: str, original_price_col: str
) -> dict:
    comparison_results = {}
    # Check for rows where CurrentPrice is greater than OriginalPrice
    if current_price_col and original_price_col:
        new_df[current_price_col] = (
            pd.to_numeric(new_df[current_price_col], errors="coerce")
            .fillna(0)
            .astype(float)
        )
        new_df[original_price_col] = (
            pd.to_numeric(new_df[original_price_col], errors="coerce")
            .fillna(0)
            .astype(float)
        )
        price_outliers = new_df[new_df[current_price_col] > new_df[original_price_col]]

        if not price_outliers.empty:
            comparison_results[current_price_col] = (
                len(price_outliers),
                f"{current_price_col} contains values greater than {original_price_col}",
            )
        else:
            comparison_results[current_price_col] = (
                0,
                f"No values greater than {original_price_col}",
            )

    return comparison_results

def get_difference(new_df, old_df):
    return {
        "All": (
            len(new_df) - len(old_df),
            "Count difference between new and old dataset",
        )
    }

def perform_qa(args,api):
    new_df = read_frame(args.new_file)
    old_df = read_frame(args.old_file)
    dataset_id = args.dataset_id

    frame = pd.DataFrame()

    # Determine critical columns if not provided
    critical_columns = args.critical_columns
    provided = True
    if not critical_columns:
        critical_columns = new_df.columns.tolist()
        provided = False
    else:
        check_critical_columns_present(new_df, critical_columns)
        check_critical_columns_present(old_df, critical_columns, new=False)


    # Determine duplicate columns if not provided
    duplicate_filter = args.duplicate_filter
    provided = True
    if not duplicate_filter:
        duplicate_filter = new_df.columns.tolist()
        provided = False
    else:
        check_critical_columns_present(new_df, duplicate_filter)

    NEW_FEED_DATE = get_feed_date(new_df)

    current_price_col = args.current_price
    original_price_col = args.original_price
    if current_price_col and original_price_col:
        check_critical_columns_present(new_df, [current_price_col, original_price_col])

    # Define a dictionary mapping each label to its corresponding function call
    processing_steps = {
        "NullCount": lambda: get_null_counts(new_df, critical_columns, provided),
        "DataType": lambda: check_outliers(new_df, old_df),
        "TotalCount": lambda: get_total_count(new_df),
        "DuplicateCount": lambda: get_duplicate_count(new_df,duplicate_filter),
        "PriceOutliersCount": lambda: check_price_columns_count(
            new_df, current_price_col, original_price_col
        ),
        "Difference": lambda: get_difference(new_df, old_df),
    }

    # Iterate over the dictionary and call each function, then add the result to the frame
    for label, func in processing_steps.items():
        column_to_value = func()
        frame = add_rows_to_frame(
            frame, column_to_value, label, NEW_FEED_DATE, dataset_id
        )

    frame.to_csv(args.output_file, index=False)
    frame = frame.to_dict(orient="records")
    api.create_stats_data(dataset_id, frame)
