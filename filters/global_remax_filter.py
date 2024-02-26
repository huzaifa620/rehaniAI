from get_data import get_data
from get_trading_data import cityElem
import pandas as pd

def global_remax_filter():

    databaseName='global_remax'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df20=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}\n{"*"*40}')

    df20['Website']='global.remax.com'
    df20.rename(columns={'title':'Title'}, inplace=True)
    df20.rename(columns={'beds':'Beds'}, inplace=True)
    df20.rename(columns={'baths':'Baths'}, inplace=True)
    df20['propertyType']=df20['propertyType'].str.replace('buy','Sale')
    df20['propertyType']=df20['propertyType'].str.replace('rent ','Rent')
    df20.rename(columns={'propertyType':'Type (Rent, Sale, Vacation)'}, inplace=True)

    cityGDP = []
    for item in df20['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df20['City GDP per Capita']=cityGDP

    population = []
    for item in df20['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df20['City Population']=population

    populationGrowthRate = []
    for item in df20['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df20['City Population Growth Rate']=populationGrowthRate

    df20 = df20.reindex(sorted(df20.columns), axis=1)

    return df20