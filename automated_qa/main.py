import argparse
from datetime import datetime
from automated_qa.config import load_token, save_token, TOKEN
from automated_qa.api import APIHandler
from automated_qa.qa import perform_qa
from automated_qa.utils import read_frame,display_datasets,valid_date

def main():
    load_token()

    parser = argparse.ArgumentParser(description="CLI to manage datasets")
    parser.add_argument("--token", type=str, help="API token for authentication")
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for displaying datasets
    parser_list = subparsers.add_parser("list", help="Display all available dataset IDs")

    parser_remove= subparsers.add_parser("remove", help="Remove a dataset ID")
    parser_remove.add_argument("id", type=int, help="Provide the dataset id to remove")

    parser_modify= subparsers.add_parser("modify", help="Modify a dataset ID")
    parser_modify.add_argument("id", type=int, help="Provide the dataset id to modify")
    parser_modify.add_argument("name", type=str, help="Name of the dataset to modify")
    parser_modify.add_argument("days", type=int, help="Frequency in days for the dataset to modify")
    parser_modify.add_argument("sites", type=int, help="Total number of sites for the dataset to modify")

    parser_remove_stat= subparsers.add_parser("remove_stat", help="Remove a dataset ID Stats")
    parser_remove_stat.add_argument("id", type=int, help="Provide the dataset id to remove")
    parser_remove_stat.add_argument("feed_date", type=valid_date, help="Provide the feed_date to remove")

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
    parser_qa.add_argument("-dc", "--duplicate_filter", nargs="+", default=None, help="List columns to check for duplicated values")
    parser_qa.add_argument('-date',"--dataset_date", type=valid_date,default=None, help="Provide the feed_date")

    args = parser.parse_args()

    # Token handling: prioritize argument, then environment variable, then prompt
    token = args.token or TOKEN
    if not token:
        token = input("Enter your API token: ")
        save_token(token)  # Save the token for future use

    # Ensure the token is available for subsequent API calls
    if not token:
        print("Error: API token is required.")
        return

    api = APIHandler(TOKEN=token)

    if args.command == "list":
        display_datasets(api)
    elif args.command == "create":
        success = api.create_dataset(name=args.name, frequency_in_days=args.days, total_sites=args.sites)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was created successfully
    elif args.command == "modify":
        success = api.modify_datasets(args.id,name=args.name, frequency_in_days=args.days, total_sites=args.sites)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was modified successfully
    elif args.command == "stats":
        perform_qa(args, api)
    elif args.command == "remove":
        api.remove_datasets(args.id)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was deleted successfully
    elif args.command == "remove_stat":
        api.remove_datasets_stats(args.id,args.feed_date)

if __name__ == "__main__":
    main()
