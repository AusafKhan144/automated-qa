import argparse
from datetime import datetime
from automated_qa.config import load_config, save_config
from automated_qa.api import APIHandler
from automated_qa.qa import perform_qa
from automated_qa.utils import read_frame,display_datasets,valid_date
from automated_qa.pipeline_prep import prepare_previous_dates_data
import os

def main():

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
    parser_modify.add_argument("first_sent_date", type=valid_date, help="Date of dataset first sent")
    parser_modify.add_argument("on_weekends", type=bool, help="set true if extract runs on weekends as well")

    parser_remove_stat= subparsers.add_parser("remove_stat", help="Remove a dataset ID Stats")
    parser_remove_stat.add_argument("id", type=int, help="Provide the dataset id to remove")
    parser_remove_stat.add_argument("feed_date", type=valid_date, help="Provide the feed_date to remove")

    # Subparser for creating a dataset
    parser_create = subparsers.add_parser("create", help="Create a new dataset")
    parser_create.add_argument("name", type=str, help="Name of the dataset to create")
    parser_create.add_argument("days", type=int, help="Frequency in days for the dataset to create")
    parser_create.add_argument("sites", type=int, help="Total number of sites for the dataset to create")
    parser_create.add_argument("first_sent_date", type=valid_date, help="Date of dataset first sent")
    parser_create.add_argument("on_weekends", type=bool, help="set true if extract runs on weekends as well")

    # Subparser for QA681bab2eae6a764dcb5befee6564db5f8c995fcb35ce3077ea0d04d346a5a62d
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

    parser_prepare = subparsers.add_parser("prepare",help="Prepare and upload the previous dates data to the cloud")
    parser_prepare.add_argument("id", type=int, help="Provide the dataset id")
    parser_prepare.add_argument("-n", "--new_file", required=True, type=str, help="Path to the new data feed CSV file")
    parser_prepare.add_argument('-date',"--dataset_date",required=True, type=valid_date,default=None, help="Provide the feed_date")

    args = parser.parse_args()

    # Token handling: prioritize argument, then environment variable, then prompt
    # Attempt to load existing configuration
    existing_token, existing_service_account_path = load_config()

    if not existing_service_account_path:
        # Prompt for service account path if not already saved
        svc_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not svc_path:
            svc_path = input('SERVICE ACCOUNT PATH: ')
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_path

        # Optionally prompt for token if not already stored
        if not existing_token:
            token_input = input('Please provide a token: ')
            save_config(token_input, svc_path)
            TOKEN = token_input
            SERVICE_ACCOUNT_PATH = svc_path
        else:
            # Token already exists, just save service account path
            save_config(existing_token, svc_path)
            SERVICE_ACCOUNT_PATH = svc_path

    api = APIHandler(TOKEN=existing_token)

    if args.command == "list":
        display_datasets(api)
    elif args.command == "create":
        success = api.create_dataset(name=args.name, frequency_in_days=args.days, total_sites=args.sites,first_sent_date=args.first_sent_date,on_weekends=args.on_weekends)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was created successfully
    elif args.command == "modify":
        success = api.modify_datasets(args.id,name=args.name, frequency_in_days=args.days, total_sites=args.sites,first_sent_date=args.first_sent_date,on_weekends=args.on_weekends)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was modified successfully
    elif args.command == "stats":
        perform_qa(args, api)
    elif args.command == "remove":
        success = api.remove_datasets(args.id)
        if success:
            display_datasets(api)  # Display the updated list if the dataset was deleted successfully
    elif args.command == "remove_stat":
        api.remove_datasets_stats(args.id,args.feed_date)
    elif args.command == "prepare":
        prepare_previous_dates_data(api,args.id, args.new_file, args.dataset_date)

if __name__ == "__main__":
    main()
