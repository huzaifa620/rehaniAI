from pymongo import MongoClient
import os

CONNECTION_STRING = os.environ.get('CONNECTION_STRING')

def get_data(databaseName):
    
    client = MongoClient(CONNECTION_STRING)
    return client[databaseName]