import json
import os

MOCK_DB_PATH = "data/mock_db.json"


class DataService:
    def __init__(self):
        self.data = {}
        self.load_data()

    def load_data(self):
        if os.path.exists(MOCK_DB_PATH):
            with open(MOCK_DB_PATH, "r") as f:
                self.data = json.load(f)
            print("Mock Data Loaded into Memory")
        else:
            print("Mock Data Not Found. Run seeder first.")

    def get_all_groups(self):
        return self.data.get("groups", {})

    def get_group_detail(self, group_id: str):
        return self.data.get("groups", {}).get(group_id)


data_service = DataService()
