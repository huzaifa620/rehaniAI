from get_data import get_data
from get_trading_data import cityElem
import pandas as pd
import numpy as np
import json, re
from google_currency import convert

def nigeriaPropertyCentre_filter():

    databaseName='nigeriaPropertyCentre'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df12=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df12['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df12['rehaniID']=hashIds  

    df12['Website'] = "nigeriapropertycenter.com"
    df12.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df12.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df12.rename(columns={'agent':'Agent'}, inplace=True)
    df12['Agent Email Address']=None
    df12.rename(columns={'beds':'Beds'}, inplace=True)
    df12.rename(columns={'baths':'Baths'}, inplace=True)
    df12['Building Security']=None
    df12.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df12['listingType']=df12['listingType'].str.replace('For ','')
    df12['listingType']=df12['listingType'].where(pd.notnull(df12['listingType']), None)
    df12.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    today = np.datetime64('today')
    daysOnMarket=(today - df12['addedOn'].values) / np.timedelta64(1, 'D')
    daysOnMarket=daysOnMarket.astype('int')
    df12['Days on Market']=daysOnMarket
    df12['Occupancy']=None
    df12['Number of Guests']=None  
    df12['Number of amenities']=None
    df12['Number of high end amenities (pool, gym, spa)']=None
    parking=[]
    for item in df12['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df12['Parking']=parking 
    kshToUsd=float(json.loads(convert('kes', 'usd', 1))['amount'])
    df12['localPrice'] = df12['price']
    df12['localCurrency'] = df12['currency']
    prices=[]
    for idx,item in enumerate(df12['price']):
        if df12['currency'][idx]=='KSh':
            item=item*kshToUsd
        prices.append(item)
    df12['price']=prices
    df12.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df12['priceDiff']):
        if df12['currency'][idx]=='KSh':
            item=item*kshToUsd
        prices.append(item)
        
    df12['pricingCriteria']=df12['pricingCriteria'].where(pd.notnull(df12['pricingCriteria']), None)
    df12['pricingCriteria']=df12['pricingCriteria'].str.replace('per ','')
    df12['pricingCriteria']=df12['pricingCriteria'].str.replace('calendar ','')
    pricingCriteria=[]
    for item in df12['pricingCriteria']:
        if item==None:
            pricingCriteria.append(None)
        elif str(item)=='nan':
            pricingCriteria.append(None)
        elif item=='':
            pricingCriteria.append(None)
        else:
            pricingCriteria.append(item.capitalize())
    df12['pricingCriteria']=pricingCriteria
    df12.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df12['priceDiff']=prices
    df12.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    internalArea=[]
    for item in df12['size']:
        if item==None:
            internalArea.append(None)
        else:
            if 'sqm' in item:
                internalArea.append(float(''.join(re.findall(r'\d+', item)))* 10.7639)
            else:
                internalArea.append(None)
    df12['size']=internalArea
    df12.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df12['Lot Area (s.f)']=None
    df12['Price per s.f.']=df12['Price']/df12['Internal Area (s.f)']
    df12['Fees (commissions, cleaning etc)']=None
    cities=[]
    for item in df12['address']:
        cities.append(item.split(',')[-1].strip())
    df12['Location: City']=cities


    district=[]
    for item in df12['address']:
        district.append(item.split(',')[-2].strip())
    df12['Location: District']=district    
    df12['Location: Country']='Nigeria'
    df12.rename(columns={'address':'Location: Address'}, inplace=True)
    df12['Location: Lat']=None
    df12['Location: Lon']=None
    df12['Location: Neighbourhood']=None

    df12['Location: City']=df12['Location: City'].str.replace('\d+', '')
    df12['Location: City']=df12['Location: City'].str.strip()
    df12['Location: City']=df12['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df12['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df12['City GDP per Capita']=cityGDP

    population = []
    for item in df12['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df12['City Population']=population

    populationGrowthRate = []
    for item in df12['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df12['City Population Growth Rate']=populationGrowthRate

    df12.drop(['propertyId','addedOn','coveredArea','currency','lastUpdated','marketStatus','parkingSpaces','toilets','totalArea'], axis=1,inplace=True)
    df12 = df12.reindex(sorted(df12.columns), axis=1)

    return df12