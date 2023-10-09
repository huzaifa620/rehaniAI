from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd
import json
from google_currency import convert

def propertypro_co_ug_filter():

    databaseName='propertypro_co_ug'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df9=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df9['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df9['rehaniID']=hashIds

    df9['Website']='propertypro.co.ug'
    df9.rename(columns={'title':'Title'}, inplace=True)
    df9.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df9.rename(columns={'agent':'Agent'}, inplace=True)
    df9['Agent Email Address']=None
    df9.rename(columns={'beds':'Beds'}, inplace=True)
    df9.rename(columns={'baths':'Baths'}, inplace=True)
    df9['Building Security']=None
    df9['Fees (commissions, cleaning etc)']=None
    amenities=[]
    for item in df9['amenities']:   
        amenities.append(len(item))
    df9['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df9['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df9['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df9['Lot Area (s.f)']=None
    df9['Number of Guests']=None
    df9['Occupancy']=None
    df9['Parking']=None
    df9['listingType']=df9['listingType'].str.replace('rent','Rent')
    df9['listingType']=df9['listingType'].str.replace('shortlet','Rent')
    df9['listingType']=df9['listingType'].str.replace('sale','Sale')
    df9.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df9['Housing Type']=None
    df9['Days on Market']=None
    df9['localPrice'] = df9['price']
    df9['localCurrency'] = df9['currency']
    ugxToUsd=float(json.loads(convert('ugx', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df9['price']):
        if df9['currency'][idx]=='UGX':
            item=item*ugxToUsd
        prices.append(item)
    df9['price']=prices
    df9.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df9['priceDiff']):
        if df9['currency'][idx]=='UGX':
            item=item*ugxToUsd
        prices.append(item)
    df9['priceDiff']=prices
    df9.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df9['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df9['Price criteria']=pricingCriteria
    df9['Price per s.f.']=None
    df9['Internal Area (s.f)']=None
    df9['Location: Lat']=None
    df9['Location: Lon']=None
    cities=[]
    for item in df9['location']:
        cities.append(item.split()[-2])
    df9['Location: City']=cities   
    df9['Location: Country']='Uganda'
    df9.rename(columns={'location':'Location: Address'}, inplace=True)
    df9['Location: District']=None
    df9['Location: Neighbourhood']=None

    cityGDP = []
    for item in df9['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df9['City GDP per Capita']=cityGDP

    population = []
    for item in df9['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df9['City Population']=population

    populationGrowthRate = []
    for item in df9['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df9['City Population Growth Rate']=populationGrowthRate

    df9.drop(['propertyId','amenities','currency','toilets','marketPriceHigher','marketPriceLower'], axis=1,inplace=True)
    df9 = df9.reindex(sorted(df9.columns), axis=1)

    return df9