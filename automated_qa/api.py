import requests
import json

class APIHandler:
    base_url = f"https://datamonitoring.watchmycompetitor.com"
    headers = {}

    def __init__(self, **kwargs):
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
            if response.status_code == 200:
                print(f"Successfully created stats data for dataset ID: {dataset_id}")
            else:
                print(f"Failed to create stats data for dataset ID: {dataset_id}")
                print(f"Status code: {response.status_code}")
                print(f"Response: {response.text}")
    
    def remove_datasets(self, dataset_id):
        response = requests.delete(f"{self.base_url}/api/dataset/{dataset_id}", headers=self.headers)
        if response.status_code == 200:
            print(f"Response: {response.json()['detail']}")
            return True
        else:
            print("Failed to remove dataset")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    def get_datasets(self):
        response = requests.get(f"{self.base_url}/api/dataset/", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to get available datasets")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return {}

    def create_dataset(self, name, frequency_in_days, total_sites):
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
            print("Failed to create dataset")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    def remove_datasets_stats(self,dataset_id,feed_date):
        response = requests.delete(f"{self.base_url}/api/stats/{dataset_id}/?feed_date={feed_date}", headers=self.headers)
        if response.status_code == 200:
            print(f"Response: {response.json()['detail']}")
            return True
        else:
            print("Failed to get available datasets")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return {}
        
    def modify_datasets(self,dataset_id, name, frequency_in_days, total_sites):
        json_data = {
            "name": name,
            "frequency_in_days": frequency_in_days,
            "total_sites": total_sites,
        }
        response = requests.put(
            f"{self.base_url}/api/dataset/{dataset_id}", headers=self.headers, json=json_data
        )
        if response.status_code == 200:
            print(f'Successfully Modified dataset with ID: {response.json()["id"]}')
            return True
        else:
            print("Failed to create dataset")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False