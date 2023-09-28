from get_data import get_data
from get_trading_data import cityElem
import pandas as pd

def propertypro_co_zw_filter():

    databaseName='propertypro_co_zw'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df16=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df16['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df16['rehaniID']=hashIds

    df16.rename(columns={'title':'Title'}, inplace=True)
    df16.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df16.rename(columns={'agent':'Agent'}, inplace=True)
    df16['Agent Email Address']=None
    df16.rename(columns={'beds':'Beds'}, inplace=True)
    df16.rename(columns={'baths':'Baths'}, inplace=True)
    df16['Building Security']=None
    df16['Fees (commissions, cleaning etc)']=None
    amenities=[]
    for item in df16['amenities']:   
        amenities.append(len(item))
    df16['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df16['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df16['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df16['Lot Area (s.f)']=None
    df16['Number of Guests']=None
    df16['Occupancy']=None
    df16['Parking']=None
    df16['listingType']=df16['listingType'].str.replace('rent','Rent')
    df16['listingType']=df16['listingType'].str.replace('shortlet','Rent')
    df16['listingType']=df16['listingType'].str.replace('sale','Sale')
    df16.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df16['Housing Type']=None
    df16['Days on Market']=None

    df16['localPrice'] = df16['price']
    df16['localCurrency'] = df16['currency']
    zidToUsd=0.0027631943 
    prices=[]
    for idx,item in enumerate(df16['price']):
        if df16['currency'][idx]=='ZID':
            item=item*zidToUsd
        prices.append(item)
    df16['price']=prices
    df16.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df16['priceDiff']):
        if df16['currency'][idx]=='ZID':
            item=item*zidToUsd
        prices.append(item)
    df16['priceDiff']=prices
    df16.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df16['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df16['Price criteria']=pricingCriteria
    df16['Price per s.f.']=None
    df16['Internal Area (s.f)']=None
    df16['Location: Lat']=None
    df16['Location: Lon']=None
    cities=[]
    for item in df16['location']:
        if item==None:
            cities.append(None)
        else:
            cities.append(item.split()[-1])
    df16['Location: City']=cities
    df16['Location: District']=None
    df16['Location: Neighbourhood']=None
    df16['Location: Country']='Zimbabwe'
    df16.rename(columns={'location':'Location: Address'}, inplace=True)

    df16['Location: City']=df16['Location: City'].str.replace('\d+', '')
    df16['Location: City']=df16['Location: City'].str.strip()
    df16['Location: City']=df16['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df16['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df16['City GDP per Capita']=cityGDP

    population = []
    for item in df16['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df16['City Population']=population

    populationGrowthRate = []
    for item in df16['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df16['City Population Growth Rate']=populationGrowthRate

    df16.drop(['propertyId','currency','amenities','marketPriceHigher','marketPriceLower','toilets'], axis=1,inplace=True)
    df16 = df16.reindex(sorted(df16.columns), axis=1)

    return df16