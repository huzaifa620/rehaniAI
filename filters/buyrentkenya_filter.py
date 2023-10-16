from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd
import numpy as np
import json
from google_currency import convert

def buyrentkenya_filter():

    databaseName='buyrentkenya'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df4=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}\n{"*"*40}')
    
    hashIds=[]
    for rawId in df4['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df4['rehaniID']=hashIds

    df4['Website']='buyrentkenya.com'
    df4.rename(columns={'title':'Title'}, inplace=True)
    df4.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df4.rename(columns={'agent':'Agent'}, inplace=True)
    df4['Agent Email Address']=None
    df4.rename(columns={'beds':'Beds'}, inplace=True)
    df4.rename(columns={'baths':'Baths'}, inplace=True)
    df4['Building Security']=None
    df4['Location: Lat']=None
    df4['Location: Lon']=None
    df4['Lot Area (s.f)']=None
    df4['Number of Guests']=None
    df4['Occupancy']=None
    df4.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df4['Type (Rent, Sale, Vacation)'] = df4['Type (Rent, Sale, Vacation)'].str.replace('sale','Sale')
    df4['Type (Rent, Sale, Vacation)'] = df4['Type (Rent, Sale, Vacation)'].str.replace('rent','Rent')
    df4.rename(columns={'parking':'Parking'}, inplace=True)
    df4['Fees (commissions, cleaning etc)']=None
    df4.rename(columns={'housingType':'Housing Type'}, inplace=True)
    amenities=[]
    for item in df4['amenities']:   
        amenities.append(len(item))
    df4['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df4['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df4['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df4.rename(columns={'city':'Location: City'}, inplace=True)
    df4['Location: Country']='Kenya'
    df4.rename(columns={'location':'Location: Address'}, inplace=True)
    df4['Location: District']=None
    conversion_factors = {'ft²': 1, 'm²': 10.7639, 'ac': 43560, 'ha': 107639}
    result = []
    for area in df4['size']:
        if area is None:
            result.append(None)
        else:
            value, unit = area.split()
            if unit in conversion_factors:
                square_feet = float(value) * conversion_factors[unit]
                result.append(square_feet)
            else:
                result.append(None)
    df4['size']=result
    df4['localPrice'] = df4['price']
    df4['localCurrency'] = df4['currency']

    df4.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    kesToUsd=float(json.loads(convert('kes', 'usd', 1))['amount'])
    df4['price']=df4['price']*kesToUsd
    df4.rename(columns={'price':'Price'}, inplace=True)
    df4['Price per s.f.']=df4['Price']/df4['Internal Area (s.f)']
    df4['priceDiff']=df4['priceDiff']*kesToUsd
    df4.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df4['Price criteria']=None
    df4.rename(columns={'suburb':'Location: Neighbourhood'}, inplace=True)
    today = np.datetime64('today')
    daysOnMarket=(today - df4['dateListed'].values) / np.timedelta64(1, 'D')
    daysOnMarket=daysOnMarket.astype('int')
    df4['Days on Market']=daysOnMarket

    df4['Location: City'] = df4['Location: City'].str.replace('Nyali','Mombasa')
    df4['Location: City'] = df4['Location: City'].str.replace('Westlands','Nairobi')
    df4['Location: City'] = df4['Location: City'].str.replace('Mombasa Island','Mombasa')
    df4['Location: City'] = df4['Location: City'].str.replace('Roysambu','Nairobi')
    df4['Location: City'] = df4['Location: City'].str.replace('Ruiru','Kiambu County')
    df4['Location: City'] = df4['Location: City'].str.replace('Limuru','Nairobi')
    df4['Location: City'] = df4['Location: City'].str.replace('Kikuyu','Kiambu County')
    df4['Location: City'] = df4['Location: City'].str.replace('Eldoret','Uasin Gishu County')
    df4['Location: City'] = df4['Location: City'].str.replace("Murang'a County",'Muranga County')

    cityGDP = []
    for item in df4['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df4['City GDP per Capita']=cityGDP

    population = []
    for item in df4['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df4['City Population']=population

    populationGrowthRate = []
    for item in df4['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df4['City Population Growth Rate']=populationGrowthRate

    df4.drop(['amenities','currency','propertyId','dateListed','marketPriceHigher','floors','marketPriceLower','numberOfUnits','projectPriceHigher','projectPriceLower','similarPropertiesInfo'], axis=1,inplace=True)
    df4 = df4.reindex(sorted(df4.columns), axis=1)

    return df4