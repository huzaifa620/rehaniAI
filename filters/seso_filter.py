from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd
import json
from google_currency import convert

def seso_filter():
    
    databaseName='SeSo'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df3=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df3['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df3['rehaniID']=hashIds  

    df3['Website']='seso.global'
    df3.rename(columns={'propertyName':'Title'}, inplace=True)
    df3['Agent']=None
    df3['Agent Contact']=None
    df3['Agent Email Address']=None
    df3['Building Security']=None
    df3['localPrice'] = df3['price']
    df3['localCurrency'] = df3['currency']
    df3.rename(columns={'beds':'Beds'}, inplace=True)
    df3.rename(columns={'baths':'Baths'}, inplace=True)
    df3.rename(columns={'price':'Price'}, inplace=True)
    df3.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df3['Location: Lat']=None
    df3['Location: Lon']=None
    df3.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df3['Type (Rent, Sale, Vacation)'] = df3['Type (Rent, Sale, Vacation)'].str.replace('SALE','Sale')
    df3['Type (Rent, Sale, Vacation)'] = df3['Type (Rent, Sale, Vacation)'].str.replace('SOLD OUT','Sold')

    usdToGhs=float(json.loads(convert('ghs', 'usd', 1))['amount'])
    usdToNgn=float(json.loads(convert('ngn', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df3['Price'].values):
        if item==None:
            prices.append(None)    
        else:
            if df3['currency'].values[idx]=='GHS':
                item=item*usdToGhs
            elif df3['currency'].values[idx]=='NGN':
                item=item*usdToNgn
            prices.append(item)
    df3['Price']=prices

    areaList=[]
    for i, area in enumerate(df3['area']):
        if area is None:
            areaList.append(None)
            continue
        area=area.replace('x ','x')
        parts = area.split()
        try:
            if 'x' in area:
                numbers=parts[0].split('x')
                areaList.append(float(numbers[0])*float(numbers[1].strip())* 10.7639)
            elif len(parts) == 2 and parts[1] == 'sqm':
                areaList.append(float(parts[0]) * 10.7639)
            else:
                areaList.append(None)
        except:
            areaList.append(None)
    df3['Internal Area (s.f)']=areaList
    df3['Price per s.f.']=df3['Price']/df3['Internal Area (s.f)']
    amenities=[]
    for item in df3['features']:
        if item==None:
            amenities.append(0)    
        else:
            amenities.append(len(item))
    df3['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df3['features']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)

    df3['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df3['Building Security']=None
    df3['Lot Area (s.f)']=None
    df3['Number of Guests']=None
    df3['Occupancy']=None
    df3['Parking']=None
    df3['Price criteria']=None
    df3['Days on Market']=None
    df3['Fees (commissions, cleaning etc)']=None
    df3.rename(columns={'propertyStatus':'Housing Type'}, inplace=True)
    df3['Days on Market']=None
    df3.rename(columns={'address':'Location: Address'}, inplace=True)
    df3['Location: Neighbourhood']=None
    df3['Location: District']=None
    df3['Location: Country']='Ghana'
    df3['Location: City']=None

    cityGDP = []
    for item in df3['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df3['City GDP per Capita']=cityGDP

    population = []
    for item in df3['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df3['City Population']=population

    populationGrowthRate = []
    for item in df3['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df3['City Population Growth Rate']=populationGrowthRate

    df3.drop(['features','currency','propertyId','area','unitsAvailable'], axis=1,inplace=True)
    df3 = df3.reindex(sorted(df3.columns), axis=1)

    return df3