from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
import os

# Load .env file
load_dotenv()

CONNECTION_STRING = os.environ.get('CONNECTION_STRING')

def send_data(df, databaseName, collectionName):
    try:
        print(f'Collected {len(df)} records!\n')
        mongo_insert_data = df.to_dict('records')

        client = MongoClient(CONNECTION_STRING)
        dbname = client[databaseName]
        collection_name = dbname[collectionName]

        with tqdm(total=len(mongo_insert_data), desc="Sending Data to MongoDB", position=0, leave=True, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as progress_bar:
            for instance in mongo_insert_data:
                try:
                    collection_name.update_one({'url': instance['url']}, {'$set': instance}, upsert=True)
                    progress_bar.update(1)
                except Exception as e:
                    print(e, instance)
            print('Data sent to MongoDB successfully')

    except Exception as e:
        print(instance)
        print('Some error occurred while sending data to MongoDB! Following is the error.')
        print(e)
        print('-----------------------------------------')
