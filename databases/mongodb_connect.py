from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoDBConnection:
    def __init__(self, uri, db_name):
        self.uri = uri
        self.db_name = db_name 
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.client.admin.command('ismaster')
            self.db = self.client[self.db_name]
            print(f"-------Connected to MongoDB database: {self.db_name}-------")
        except ConnectionFailure as e:
            print(f"-------Could not connect to MongoDB: {e}-------")
            raise

    def close(self):
        if self.client:
            self.client.close()
            print("-------MongoDB connection closed.-------")
    
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()