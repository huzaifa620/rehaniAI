from pymongo import MongoClient
import os

CONNECTION_STRING = os.environ.get('CONNECTION_STRING')

def get_trading_data():
    
    client = MongoClient(CONNECTION_STRING)
    db = client['economicIndicators']
    collection1 = db['countries']
    collection2 = db['cities']
    data1 = collection1.find()
    data2 = collection2.find()
    return list(data1), list(data2)

def countryElem(countryName):
        return next((i for i in countriesDb if i['countryName'] == countryName), None)

def cityElem(cityName):
    return next((i for i in citiesDb if i['cityName'] == cityName), None)