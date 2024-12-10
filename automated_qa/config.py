import json
import os

CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cli_app_config.json")
TOKEN = None
SERVICE_ACCOUNT_PATH = None

def load_config():
    global TOKEN, SERVICE_ACCOUNT_PATH
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as file:
            config = json.load(file)
            TOKEN = config.get("token")
            SERVICE_ACCOUNT_PATH = config.get("service_account_path")
            if SERVICE_ACCOUNT_PATH:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
            return TOKEN, SERVICE_ACCOUNT_PATH
    return None, None

def save_config(token, service_account_path):
    with open(CONFIG_PATH, 'w') as file:
        json.dump({"token": token, "service_account_path": service_account_path}, file)


