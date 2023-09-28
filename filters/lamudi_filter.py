from get_data import get_data
from get_trading_data import cityElem
import pandas as pd
import json, re
from google_currency import convert

def lamudi_filter():

    databaseName='lamudi'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df11=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df11['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df11['rehaniID']=hashIds

    df11.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df11.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df11.rename(columns={'agent':'Agent'}, inplace=True)
    df11.rename(columns={'agentEmail':'Agent Email Address'}, inplace=True)
    df11.rename(columns={'beds':'Beds'}, inplace=True)
    df11.rename(columns={'baths':'Baths'}, inplace=True)
    df11['Building Security']=None
    df11['Fees (commissions, cleaning etc)']=None
    amenities=[]
    for item in df11['amenities']:   
        amenities.append(len(item))
    df11['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df11['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df11['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df11.rename(columns={'category':'Housing Type'}, inplace=True)
    df11['Lot Area (s.f)']=None
    df11['Number of Guests']=None
    df11['Occupancy']=None
    df11['Parking']=None

    df11['listingType']=df11['listingType'].str.replace('For sale','Sale')
    df11['listingType']=df11['listingType'].str.replace('For rent','Rent')
    df11.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df11['Days on Market']=None
    
    ugxToUsd=float(json.loads(convert('ugx', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df11['price']):
        if df11['currency'][idx]=='Ugx':
            item=item*ugxToUsd
        prices.append(item)

    df11['localPrice'] = df11['price']
    df11['localCurrency'] = df11['currency']
    df11['price']=prices
    df11.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df11['priceDiff']):
        if df11['currency'][idx]=='Ugx':
            item=item*ugxToUsd
        prices.append(item)
    df11['priceDiff']=prices
    df11.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    priceCriteria=[]
    for item in (df11['Type (Rent, Sale, Vacation)']):
        if item=='Rent':
            priceCriteria.append('Month')
        else:
            priceCriteria.append(None)
    df11['Price criteria']=priceCriteria  
    unitsConv={
        'Decimals':435.56,
        'Sq Meters':10.7639,
        'Sq Miles':27880000,
        'Acres':43560,
        'Units':1,
        'Sq Feet':1,
    }
    converted_sizes=[]
    for item in df11['size']:
        if item==None:
            converted_sizes.append(None)
        else:
            try:
                converted_sizes.append(float(item.split()[0])*unitsConv[re.sub('\d', '', item.replace('.','')).strip()])
            except:
                converted_sizes.append(None)
    df11['Internal Area (s.f)']=converted_sizes
    df11['Price per s.f.']=df11['Price']/df11['Internal Area (s.f)']
    df11['Location: Country']='Uganda'
    df11['Location: Address']=df11['location']
    df11.rename(columns={'location':'Location: District'}, inplace=True)
    df11.rename(columns={'district':'Location: City'}, inplace=True)
    df11['Location: Lat']=None
    df11['Location: Lon']=None
    df11['Location: Neighbourhood']=None
    df11['Location: City']=df11['Location: City'].str.replace('\d+', '')
    df11['Location: City']=df11['Location: City'].str.strip()
    df11['Location: City']=df11['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df11['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df11['City GDP per Capita']=cityGDP

    population = []
    for item in df11['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df11['City Population']=population

    populationGrowthRate = []
    for item in df11['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df11['City Population Growth Rate']=populationGrowthRate

    df11.drop(["propertyId","amenities","currency","size","tenure"], axis=1,inplace=True)
    df11 = df11.reindex(sorted(df11.columns), axis=1)

    return df11