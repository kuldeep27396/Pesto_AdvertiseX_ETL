from utils import load_to_database

class DataLoader:
    def __init__(self, processed_data, db_connection_string):
        self.processed_data = processed_data
        self.db_connection_string = db_connection_string

    def load(self):
        load_to_database(self.processed_data, self.db_connection_string)

if __name__ == "__main__":
    loader = DataLoader(processed_data, 'your_database_connection_string')
    loader.load()
