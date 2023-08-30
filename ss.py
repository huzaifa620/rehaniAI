from pymongo import MongoClient

def trim_collections(client, max_docs=60000):
    for db_name in client.list_database_names():
        db = client[db_name]
        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            doc_count = collection.count_documents({})
            if doc_count > max_docs:
                excess_docs = doc_count - max_docs
                print(f"Trimming {excess_docs} documents from '{collection_name}' in '{db_name}'")
                excess_documents = collection.find({}).limit(excess_docs)
                excess_ids = [doc['_id'] for doc in excess_documents]
                collection.delete_many({'_id': {'$in': excess_ids}})

if __name__ == "__main__":
    
    connection_string = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
    
    client = MongoClient(connection_string)
    trim_collections(client, max_docs=60000)
    client.close()
