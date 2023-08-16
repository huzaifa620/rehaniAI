from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

CONNECTION_STRING = os.environ.get('CONNECTION_STRING')

def send_data(df, databaseName, collectionName):
    try:
        print(f'Collected {len(df)} records!')
        mongo_insert_data = df.to_dict('records')
        print('Sending Data to MongoDB!')

        client = MongoClient(CONNECTION_STRING)
        dbname = client[databaseName]
        collection_name = dbname[collectionName]

        for instance in mongo_insert_data:
            collection_name.update_one({'rehaniID': instance['rehaniID']}, {'$set': instance}, upsert=True)
        print('Data sent to MongoDB successfully')

    except Exception as e:
        print(instance)
        print('Some error occurred while sending data to MongoDB! Following is the error.')
        print(e)
        print('-----------------------------------------')
