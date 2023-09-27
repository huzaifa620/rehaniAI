from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd
import json
from google_currency import convert

def propertypro_co_ke_filter():

    databaseName='propertypro_co_ke'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df8=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df8['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df8['rehaniID']=hashIds

    df8['Website']='propertypro.co.ke'
    df8.rename(columns={'title':'Title'}, inplace=True)
    df8.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df8.rename(columns={'agent':'Agent'}, inplace=True)
    df8['Agent Email Address']=None
    df8.rename(columns={'beds':'Beds'}, inplace=True)
    df8.rename(columns={'baths':'Baths'}, inplace=True)
    df8['Building Security']=None
    df8['Fees (commissions, cleaning etc)']=None
    amenities=[]
    for item in df8['amenities']:   
        amenities.append(len(item))
    df8['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df8['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df8['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df8['Lot Area (s.f)']=None
    df8['Number of Guests']=None
    df8['Occupancy']=None
    df8['Parking']=None
    df8['listingType']=df8['listingType'].str.replace('rent','Rent')
    df8['listingType']=df8['listingType'].str.replace('shortlet','Rent')
    df8['listingType']=df8['listingType'].str.replace('sale','Sale')
    df8.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df8['Housing Type']=None
    df8['Days on Market']=None
    df8['localPrice'] = df8['Price']
    df8['localCurrency'] = df8['currency']
    kesToUsd=float(json.loads(convert('kes', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df8['price']):
        if df8['currency'][idx]=='KES':
            item=item*kesToUsd
        prices.append(item)
    df8['price']=prices
    df8.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df8['priceDiff']):
        if df8['currency'][idx]=='KES':
            item=item*kesToUsd
        prices.append(item)
    df8['priceDiff']=prices
    df8.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df8['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df8['Price criteria']=pricingCriteria
    df8['Price per s.f.']=None
    df8['Internal Area (s.f)']=None
    df8['Location: Lat']=None
    df8['Location: Lon']=None
    cities=[]
    for item in df8['location']:
        cities.append(item.split()[-1])
    df8['Location: City']=cities
    df8['Location: District']=None
    df8['Location: Neighbourhood']=None
    df8['Location: Country']='Kenya'
    df8.rename(columns={'location':'Location: Address'}, inplace=True)
    df8['Location: City']=df8['Location: City'].str.replace('Gishu','Uasin Gishu')
    df8['Location: City']=df8['Location: City'].str.replace('Bay','Homa Bay')
    df8['Location: City']=df8['Location: City'].str.replace("Murang'a",'Muranga')

    cityGdpCapita={
     'Mombasa': cityElem('Mombasa')['gdpPerCapita'],
     'Nairobi': cityElem('Nairobi')['gdpPerCapita'],
     'Kwale': 2032,
     'Kiambu': 4422,
     'Kilifi': 1645,
     'Kajiado': 2387,
     'Machakos': 3863,
     'Nakuru County': cityElem('Nakuru')['gdpPerCapita'],
     'Uasin Gishu County': cityElem('Uasin Gishu County')['gdpPerCapita'],
     'Laikipia': 3092,
     'Lamu': 4880,
     'Taita-Taveta': 2777,
     'Kisumu': cityElem('Kisumu')['gdpPerCapita'],
     'Nyeri': 4291,
     'Migori': 1756,
     'Siaya': 1891,
     'Narok': 3206,
     'Muranga': 3455,
     'Meru': 3086,
     'Nyandarua': 6955,
     'Trans Nzoia': 2169,
     'Kakamega': 1910,
     'Homa Bay': 1981,
     'Kirinyaga': 3248,
     'Kitui': 1829,
     'Embu': 3662,
     'Kericho': 2816,
     'Makueni':2080,
     'Busia':3089,
     'Baringo':2545,
     'Samburu':1800,
     'Nandi':2419,
     'Bungoma':1957,
    }
    gdpCapita=[]
    for item in df8['Location: City'].values:
        if item not in cityGdpCapita:
            gdpCapita.append(None)
        else:
            gdpCapita.append(cityGdpCapita[item])
    df8['City GDP per Capita']=gdpCapita

    populationCity={
     'Mombasa': cityElem('Mombasa')['population'],
     'Nairobi': cityElem('Nairobi')['population'],
     'Kwale': 866820,
     'Kiambu': 2417735,
     'Kilifi': 1453787,
     'Kajiado': 1117840,
     'Machakos': 1421932,
     'Nakuru County': cityElem('Nakuru')['population'],
     'Uasin Gishu County': cityElem('Uasin Gishu County')['population'],
     'Laikipia':518560,
     'Lamu': 143920,
     'Taita-Taveta': 340671,
     'Kisumu': cityElem('Kisumu')['population'],
     'Nyeri': 759164,
     'Migori':1116436,
     'Siaya':993183 ,
     'Narok': 1157873,
     'Muranga': 1056640,
     'Meru': 1545714,
     'Nyandarua': 638289,
     'Trans Nzoia': 990341,
     'Kakamega': 1867579,
     'Homa Bay': 1131950,
     'Kirinyaga': 610411,
     'Kitui': 1136187,
     'Embu': 608599,
     'Kericho': 901777,
     'Makueni':987653,
     'Busia':893681,
     'Baringo':666763 ,
     'Samburu':310327,
     'Nandi':885711,
     'Bungoma':1670570,
    }
    population=[]
    for item in df8['Location: City'].values:
        if item not in populationCity:
            population.append(None)
        else:
            population.append(populationCity[item])        
    df8['City Population']=population

    cityGdpGrowthRate={
     'Mombasa': 4.7,
     'Nairobi': 21.7,
     'Kwale': 1.1,
     'Kiambu': 5.5,
     'Kilifi': 1.6,
     'Kajiado': 1.5,
     'Machakos': 3.2,
     'Nakuru': 6.1,
     'Uasin Gishu': 2.3,
     'Laikipia': 1.0,
     'Lamu': 0.4,
     'Taita-Taveta': 0.7,
     'Kisumu': 2.9,
     'Nyeri': 2.2,
     'Migori': 1.2,
     'Siaya': 1.2,
     'Narok': 2.2,
     'Muranga': 2.3,
     'Meru': 2.9,
     'Nyandarua': 2.6,
     'Trans Nzoia': 1.7,
     'Kakamega': 2.4,
     'Homa Bay': 1.4,
     'Kirinyaga': 1.4,
     'Kitui': 1.4,
     'Embu': 1.4,
     'Kericho': 1.8,
     'Makueni':5.5,
     'Busia':1.0,
     'Baringo':1.1,
     'Samburu':0.3,
     'Nandi':1.6,
     'Bungoma':2.3,
    }
    growthRate=[]
    for item in df8['Location: City'].values:
        if item not in cityGdpGrowthRate:
            growthRate.append(None)
        else:
            growthRate.append(cityGdpGrowthRate[item])
    df8['City Population Growth Rate']=growthRate

    df8.drop(['propertyId','amenities','currency','toilets','marketPriceHigher','marketPriceLower'], axis=1,inplace=True)
    df8 = df8.reindex(sorted(df8.columns), axis=1)

    return df8