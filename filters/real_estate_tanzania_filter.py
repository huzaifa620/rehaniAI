from get_data import get_data
from get_trading_data import cityElem
import pandas as pd
import numpy as np
import json, re
from google_currency import convert

def real_estate_tanzania_filter():

    databaseName='realEstateTanzania'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df18=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}\n{"*"*40}')

    # hashIds=[]
    # for rawId in df18['url']:
    #     hashId=hash(rawId)
    #     if hashId<0:
    #         hashId=int(str(hashId).replace('-','1'))
    #     hashIds.append(hashId)
    # df18['rehaniID']=hashIds

    df18['Website']='realestatetanzania.com'
    df18.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df18['agentNumber']=df18['agentNumber'].str.replace(' ','')
    df18.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df18.rename(columns={'agent':'Agent'}, inplace=True)
    df18['Agent Email Address']=None
    df18.rename(columns={'beds':'Beds'}, inplace=True)
    df18.rename(columns={'baths':'Baths'}, inplace=True)
    df18['Building Security']=None
    df18['Fees (commissions, cleaning etc)']=None
    df18['Number of amenities']=None
    df18['Number of high end amenities (pool, gym, spa)']=None
    df18['Lot Area (s.f)']=None
    df18['Number of Guests']=None
    df18['Occupancy']=None
    df18['Parking']=None
    df18['listingType']=df18['listingType'].str.replace('For ','')
    df18['listingType']=df18['listingType'].str.replace('Sold ','Sale')
    df18['listingType']=df18['listingType'].str.replace('Rented ','Rent')
    df18['listingType']=df18['listingType'].replace('Not Available ',None)
    df18.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)

    # today = np.datetime64('today')
    # daysOnMarket=(today - df18['dateUpdated'].values) / np.timedelta64(1, 'D')
    # daysOnMarket=daysOnMarket.astype('int')
    # df18['Days on Market']=daysOnMarket
    df18.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    tzsToUsd=float(json.loads(convert('tzs', 'usd', 1))['amount'])
    df18.rename(columns={'dateUpdated':'dateAdded'}, inplace=True)

    df18['localPrice'] = df18['price']
    df18['localCurrency'] = df18['currency']
    prices=[]
    for idx,item in enumerate(df18['price']):
        if df18['currency'][idx]=='TZS' or df18['currency'][idx]=='TSZ':
            item=item*tzsToUsd     
        prices.append(item)
    df18['price']=prices
    df18.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df18['priceDiff']):
        if df18['currency'][idx]=='TZS' or df18['currency'][idx]=='TSZ':
            item=item*tzsToUsd     
        prices.append(item)
    df18['priceDiff']=prices
    #df18.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    converted_size=[]
    for i in df18['size']:
        if str(i)=='nan' or i==None:
            converted_size.append(None)
            continue
        i=i.lower().replace(' ','').replace(',','')
        if 'sqm' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqmt')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqmt')[0])[0])*10.7639
            else:
                size=None
            converted_size.append(size)
        elif 'acre' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('acre')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('acre')[0])[0])*43560
            else:
                size=None        
            converted_size.append(size)
        elif 'sqft' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqft')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqft')[0])[0])
            else:
                size=None
            converted_size.append(size)
        elif 'm²' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('m²')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('m²')[0])[0])*10.7639
            else:
                size=None
            converted_size.append(size)
        else:
            converted_size.append(None)
        
    df18['plotSize']=converted_size       
    df18['Price per s.f.']=df18['Price']/df18['plotSize']
    pricingCriteria=[]
    for i in df18['pricingCriteria']:
        if i==None:
            pricingCriteria.append(None)
            continue
        if 'month' in i.lower():
            if 'six' or '6' in i.lower():
                pricingCriteria.append('6 Months')
            else:
                pricingCriteria.append('Month')
        elif 'year' in i.lower():
            pricingCriteria.append('Year')
        elif 'day' in i.lower():
            pricingCriteria.append('Day')
        else:
            pricingCriteria.append(None)
    df18['Price criteria']=pricingCriteria    
    df18['Location: Country']='Tanzania'
    df18['Location: Lat']=None
    df18['Location: Lon']=None
    df18.rename(columns={'address':'Location: Address'}, inplace=True)
    df18.rename(columns={'city':'Location: City'}, inplace=True)
    df18['Location: District']=None
    df18['Location: Neighbourhood']=None
    df18['Location: City']=df18['Location: City'].str.replace('\d+', '')
    df18['Location: City']=df18['Location: City'].str.strip()
    df18['Location: City']=df18['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df18['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df18['City GDP per Capita']=cityGDP

    population = []
    for item in df18['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df18['City Population']=population

    populationGrowthRate = []
    for item in df18['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df18['City Population Growth Rate']=populationGrowthRate

    df18.drop(["propertyId","currency","location","pricingCriteria","size","state"], axis=1,inplace=True)
    df18 = df18.reindex(sorted(df18.columns), axis=1)

    return df18