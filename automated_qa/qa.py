from datetime import datetime
import pandas as pd
import requests
import argparse
import json
import os

TOKEN = None
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cli_app_config.json")

def load_token():
    global TOKEN
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as file:
            config = json.load(file)
            TOKEN = config.get("token")

def save_token(token):
    with open(CONFIG_PATH, 'w') as file:
        json.dump({"token": token}, file)

class APIHandler:
    env = "prd"
    base_url = f"http://datamonitoring.wmc-platform-{env}.intranet/"
    headers = {}

    def __init__(self,**kwargs):
        token = kwargs.get("TOKEN")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def create_stats_data(self, dataset_id, jdata):
        for json_data in jdata:
            response = requests.post(
                f"{self.base_url}/api/stats/{dataset_id}",
                headers=self.headers,
                data=json.dumps(json_data),
            )
            print(json_data)
            if response.status_code == 200:
                print(f"Successfully created stats data for dataset ID: {dataset_id}")
                # return True
            else:
                print(f"Failed to create stats data for dataset ID: {dataset_id}")
                print(f"Status code: {response.status_code}")
                print(f"Response: {response.text}")
                # return False

    def get_datasets(self):
        response = requests.get(f"{self.base_url}/api/dataset/", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get available datasets")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return {}

    def create_dataset(self, **kwargs):

        name = kwargs.get("name")
        frequency_in_days = kwargs.get("frequency_in_days")
        total_sites = kwargs.get("total_sites")

        json_data = {
            "name": name,
            "frequency_in_days": frequency_in_days,
            "total_sites": total_sites,
        }

        response = requests.post(
            f"{self.base_url}/api/dataset/", headers=self.headers, json=json_data
        )

        if response.status_code == 200:
            print(f'Successfully created dataset with ID: {response.json()["id"]}')
            return True
        else:
            print(f"Failed to create dataset")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False


def display_datasets(api):
    datasets = api.get_datasets()
    if datasets:
        print("Available Dataset IDs:")
        for dataset in datasets:
            print(f"ID: {dataset['id']} - Name: {dataset.get('name', 'Unnamed')}")
    else:
        print("No datasets found.")


def read_frame(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    except:
        df = pd.read_csv(path, encoding="latin", low_memory=False)
    return df


def get_feed_date(df):
    formats = ["%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"]
    for format in formats:
        try:
            current_date = datetime.strptime(
                str(df.DateExtractRun.unique().tolist()[0]), format
            ).strftime("%Y-%m-%d")
        except ValueError:
            continue
        return current_date


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


def get_duplicate_count(df):
    return {"All": (df.duplicated().sum(), "Count of duplicate rows in the dataset")}


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


def add_rows_to_frame(
    frame: pd.DataFrame,
    data_dict: dict,
    stats_type: str,
    feed_date: str,
    dataset_id: int,
):
    rows = []
    for column, value in data_dict.items():
        val = value[0]
        reason = value[1]
        row = {}
        row["type"] = stats_type
        row["value"] = val
        row["reason"] = reason
        row["related_columns"] = column
        row["dataset_id"] = dataset_id
        row["feed_date"] = feed_date
        rows.append(row)

    frame = pd.concat([frame, pd.DataFrame(rows)], ignore_index=True)
    return frame


def check_critical_columns_present(frame, columns, new=True):
    frame_cols = frame.columns
    for col in columns:
        if col not in frame_cols:
            if new:
                exit(f"Critical column '{col}' not found in the new file.")
            else:
                exit(f"Critical column '{col}' not found in the old file.")


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
        "DuplicateCount": lambda: get_duplicate_count(new_df),
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


# Set up argparse for CLI arguments
def main():
    global TOKEN

    # Load token from configuration file if available
    load_token()


    parser = argparse.ArgumentParser(description="CLI to manage datasets")
    parser.add_argument("--token", type=str, help="API token for authentication")
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for displaying datasets
    parser_list = subparsers.add_parser("list", help="Display all available dataset IDs")

    # Subparser for creating a dataset
    parser_create = subparsers.add_parser("create", help="Create a new dataset")
    parser_create.add_argument("name", type=str, help="Name of the dataset to create")
    parser_create.add_argument("days", type=int, help="Frequency in days for the dataset to create")
    parser_create.add_argument("sites", type=int, help="Total number of sites for the dataset to create")

    # Subparser for QA
    parser_qa = subparsers.add_parser("stats", help="Perform QA on a dataset")
    parser_qa.add_argument("-n", "--new_file", required=True, type=str, help="Path to the new data feed CSV file")
    parser_qa.add_argument("-o", "--old_file", required=True, type=str, help="Path to the old data feed CSV file")
    parser_qa.add_argument("-d", "--dataset_id", required=True, type=int, help="Provide the dataset id")
    parser_qa.add_argument("-r", "--output_file", default="output.csv", type=str, help="Path for the output Excel file")
    parser_qa.add_argument("-cp", "--current_price", type=str, default=None, help="Current price column in the data")
    parser_qa.add_argument("-op", "--original_price", type=str, default=None, help="Original price column in the data")
    parser_qa.add_argument("-c", "--critical_columns", nargs="+", default=None, help="List of critical columns to check for null values")

    args = parser.parse_args()

    # Token handling: prioritize argument, then environment variable, then prompt
    if args.token:
        TOKEN = args.token
        save_token(TOKEN)  # Save token for future use
    elif not TOKEN:
        TOKEN = input("Enter your API token: ")
        if TOKEN:
            save_token(TOKEN)  # Save token if provided by user input

    # Ensure the token is available for subsequent API calls
    if not TOKEN:
        print("Error: API token is required.")
        return

    api = APIHandler(TOKEN=TOKEN)

    if args.command == "list":
        display_datasets(api)
    elif args.command == "create":
        success = api.create_dataset(name=args.name, frequency_in_days=args.days, total_sites=args.sites)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was created successfully
    elif args.command == "stats":
        perform_qa(args, api)


if __name__ == "__main__":
    main()
