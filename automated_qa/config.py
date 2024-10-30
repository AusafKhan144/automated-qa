import json
import os

CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cli_app_config.json")
TOKEN = None

def load_token():
    global TOKEN
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as file:
            config = json.load(file)
            TOKEN = config.get("token")
            return TOKEN
    return None

def save_token(token):
    with open(CONFIG_PATH, 'w') as file:
        json.dump({"token": token}, file)
