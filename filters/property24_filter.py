from get_data import get_data
from get_trading_data import cityElem
import pandas as pd
from google_currency import convert
import json

def property24_filter():

    databaseName='property24'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df14=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}\n{"*"*40}')

    hashIds=[]
    for rawId in df14['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df14['rehaniID']=hashIds

    df14['Website'] = "property24.com"
    df14.rename(columns={'title':'Title'}, inplace=True)
    df14['Agent Email Address']=None
    df14['Agent Contact']=None
    df14.rename(columns={'agent':'Agent'}, inplace=True)
    df14.rename(columns={'beds':'Beds'}, inplace=True)
    df14.rename(columns={'baths':'Baths'}, inplace=True)
    df14['Building Security']=None
    df14['Days on Market']=None
    df14['Lot Area (s.f)']=None
    df14['Number of Guests']=None
    df14['Number of amenities']=None
    df14['Number of high end amenities (pool, gym, spa)']=None
    df14['Fees (commissions, cleaning etc)']=None
    df14['Housing Type']=None
    df14['priceChange']=None
    df14['priceStatus']=None
    df14['Price Change']=None
    df14['Price criteria']=None
    df14['Occupancy']=None
    unitsDict={
        'm²':10.7639,
        'ha':107639,
        'acres':43560,
        'acre':43560,
    }
    converted_sizes=[]
    for item in df14['erfSize']:
        if str(item)=='nan':
            converted_sizes.append(None)
        else:
            converted_sizes.append(float(''.join(item.split()[:-1]).replace(' ',''))*unitsDict[item.split()[-1]])
    df14['size']=converted_sizes
    df14.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)    
    parkingStatuses=[]
    for i in df14['parking']:
        if str(i)=='nan':
            parkingStatuses.append(False)
        else:
            parkingStatuses.append(True)
    df14['parking']=parkingStatuses
    df14.rename(columns={'parking':'Parking'}, inplace=True)   
    df14['listingType']=df14['listingType'].str.replace('sale','Sale')
    df14['listingType']=df14['listingType'].str.replace('rent','Rent')
    df14.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df14.rename(columns={'imgUrl':'imgUrls'}, inplace=True)
    zarToUsd=float(json.loads(convert('zar', 'usd', 1))['amount'])

    df14['localPrice'] = df14['price']
    df14['localCurrency'] = df14['currency']
    prices=[]
    for idx,item in enumerate(df14['price']):
        if df14['currency'][idx]=='ZAR':
            item=item*zarToUsd     
        prices.append(item)
    df14['price']=prices
    df14.rename(columns={'price':'Price'}, inplace=True)
    df14['Price per s.f.']=df14['Price']/df14['Internal Area (s.f)']
    df14['Location: Country']='South Africa'
    df14.rename(columns={'city':'Location: City'}, inplace=True)
    df14.rename(columns={'district':'Location: District'}, inplace=True)
    df14['Location: Address']=None
    df14['Location: Lat']=None
    df14['Location: Lon']=None
    df14['Location: Neighbourhood']=None

    df14['Location: City']=df14['Location: City'].str.replace('\d+', '')
    df14['Location: City']=df14['Location: City'].str.strip()
    df14['Location: City']=df14['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df14['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df14['City GDP per Capita']=cityGDP

    population = []
    for item in df14['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df14['City Population']=population

    populationGrowthRate = []
    for item in df14['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df14['City Population Growth Rate']=populationGrowthRate

    df14.drop(['propertyId','currency','erfSize','floorSize','availability','country'], axis=1,inplace=True)
    df14 = df14.reindex(sorted(df14.columns), axis=1)

    return df14