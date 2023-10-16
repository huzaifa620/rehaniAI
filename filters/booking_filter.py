from get_data import get_data
from get_trading_data import cityElem
import pandas as pd
import re

def booking_filter():

    databaseName='booking'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    print(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df19=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}\n{"*"*40}')

    hashIds=[]
    for rawId in df19['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df19['rehaniID']=hashIds

    df19['Website'] = "booking.com"
    df19.rename(columns={'title':'Title'}, inplace=True)
    df19['Agent']=None
    df19['Agent Contact']=None
    df19['Agent Email Address']=None
    bedsQuantity=[]
    for item in df19['beds']:
        if str(item)=='nan' or item==None:
            bedsQuantity.append(None)
            continue
        bedsQuantity.append(len(item))
    df19['Beds']=bedsQuantity
    bathrooms=[]
    for idx,item in enumerate(df19['amenities']):
        if item==None or str(item)=='nan':
            bathrooms.append(None)
            continue
        if 'Attached bathroom' in item:        
            if df19['beds'][idx]==None or str(df19['beds'][idx])=='nan':
                bathrooms.append(None)
            else:
                counter=0
                for rooms in df19['beds'][idx]:
                    if 'bed' in rooms.lower():
                        counter=counter+1
                bathrooms.append(counter)
        else:
            bathrooms.append(None)            
    df19['Baths']=bathrooms
    df19['Building Security']=None
    df19.rename(columns={'images':'imgUrls'}, inplace=True)
    df19['localPrice'] = df19['price']
    df19['localCurrency'] = df19['currency']
    df19.rename(columns={'price':'Price'}, inplace=True)
    df19.rename(columns={'roomType':'Housing Type'}, inplace=True)
    df19['Type (Rent, Sale, Vacation)']='Vacations'
    df19['Days on Market']=None
    df19['Fees (commissions, cleaning etc)']=None
    df19['Lot Area (s.f)']=None
    df19.rename(columns={'sleeps':'Number of Guests'}, inplace=True)
    df19['Occupancy']=None
    df19['Parking']=None
    converted_size=[]
    for i in df19['size']:
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
          
    df19['Internal Area (s.f)']=converted_size
    df19.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df19['Price per s.f.']=df19['Price']/df19['Internal Area (s.f)']
    df19.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    amenities=[]
    for item in df19['amenities']:
        if item==None:
            amenities.append(0)    
        else:
            amenities.append(len(item))
    df19['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df19['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df19['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df19['Location: Lat']=None
    df19['Location: Lon']=None
    df19.rename(columns={'city':'Location: City'}, inplace=True)
    df19.rename(columns={'country':'Location: Country'}, inplace=True)
    df19.rename(columns={'address':'Location: Address'}, inplace=True)

    df19['Location: District']=None
    df19['Location: Neighbourhood']=None
    df19['Location: City']=df19['Location: City'].str.replace('\d+', '')
    df19['Location: City']=df19['Location: City'].str.strip()
    df19['Location: City']=df19['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df19['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df19['City GDP per Capita']=cityGDP

    population = []
    for item in df19['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df19['City Population']=population

    populationGrowthRate = []
    for item in df19['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df19['City Population Growth Rate']=populationGrowthRate

    df19.drop(["variantId","avgPrice","amenities","areaInfo","balcony","beds","breakfast","breakfastIncluded","cancellationPolicy","categoryRating","checkIn","checkOut","closestAirports","currency","discountPercent","features","highlights","mainUrl","prePayment","propertyId","rating","refundPolicy","reviews","roomAvailability","savings","size","stars","taxAmount","taxesIncluded","views"], axis=1,inplace=True)
    df19 = df19.reindex(sorted(df19.columns), axis=1)

    return df19