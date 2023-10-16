from get_data import get_data
from get_trading_data import cityElem
import pandas as pd
import json
from google_currency import convert

def propertypro_ng_filter():

    databaseName='propertypro_ng'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df17=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}\n{"*"*40}')

    hashIds=[]
    for rawId in df17['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df17['rehaniID']=hashIds

    df17['Website']='propertypro.co.ng'
    df17.rename(columns={'title':'Title'}, inplace=True)
    df17.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df17.rename(columns={'agent':'Agent'}, inplace=True)
    df17['Agent Email Address']=None
    df17.rename(columns={'beds':'Beds'}, inplace=True)
    df17.rename(columns={'baths':'Baths'}, inplace=True)
    df17['Building Security']=None
    df17['Fees (commissions, cleaning etc)']=None
    amenities=[]
    for item in df17['amenities']:   
        amenities.append(len(item))
    df17['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df17['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df17['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df17['Lot Area (s.f)']=None
    df17['Number of Guests']=None
    df17['Occupancy']=None
    df17['Parking']=None
    df17['listingType']=df17['listingType'].str.replace('rent','Rent')
    df17['listingType']=df17['listingType'].str.replace('shortlet','Rent')
    df17['listingType']=df17['listingType'].str.replace('sale','Sale')
    df17.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df17['Housing Type']=None
    df17['Days on Market']=None
    df17['localPrice'] = df17['price']
    df17['localCurrency'] = df17['currency']
    ngnToUsd=float(json.loads(convert('ngn', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df17['price']):
        if df17['currency'][idx]=='NGN':
            item=item*ngnToUsd
        prices.append(item)
    df17['price']=prices
    df17.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df17['priceDiff']):
        if df17['currency'][idx]=='NGN':
            item=item*ngnToUsd
        prices.append(item)
    df17['priceDiff']=prices
    df17.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df17['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df17['Price criteria']=pricingCriteria
    df17['Price per s.f.']=None
    df17['Internal Area (s.f)']=None
    df17['Location: Lat']=None
    df17['Location: Lon']=None     
    cities=[]
    for item in df17['location']:
        if item==None:
            cities.append(None)
        else:
            cities.append(item.split()[-1])
    df17['Location: City']=cities
    df17['Location: District']=None
    df17['Location: Neighbourhood']=None
    df17['Location: Country']='Nigeria'
    df17.rename(columns={'location':'Location: Address'}, inplace=True)

    df17['Location: City']=df17['Location: City'].str.replace('\d+', '')
    df17['Location: City']=df17['Location: City'].str.strip()
    df17['Location: City']=df17['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df17['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df17['City GDP per Capita']=cityGDP

    population = []
    for item in df17['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df17['City Population']=population

    populationGrowthRate = []
    for item in df17['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df17['City Population Growth Rate']=populationGrowthRate

    df17.drop(['propertyId','currency','amenities','marketPriceHigher','marketPriceLower','toilets'], axis=1,inplace=True)
    df17 = df17.reindex(sorted(df17.columns), axis=1)

    return df17