from get_data import get_data
from get_trading_data import cityElem
import pandas as pd

def mubawab_filter():

    databaseName='mubawab'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df13=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df13['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df13['rehaniID']=hashIds  

    def convert_to_sqft(area):
        if area is None:
            return None
        sqm = float(area[:-3])  # Extract numerical value as float
        sqft = sqm * 10.7639  # Convert square meters to square feet
        return round(sqft, 2)

    df13['website'] = "mubawab"
    df13.rename(columns={'title':'Title'}, inplace=True)
    df13.rename(columns={'agent':'Agent'}, inplace=True)
    df13['Agent Email Address']=None
    df13.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df13['Agent Contact']=df13['Agent Contact'].str.replace(' ','')
    df13['baths']=df13['baths'].str.replace('Bathrooms','')
    df13['baths']=df13['baths'].str.replace('Bathroom','')
    df13['beds']=df13['beds'].str.replace('Rooms','')
    df13['beds']=df13['beds'].str.replace('Room','')

    df13.rename(columns={'beds':'Beds'}, inplace=True)
    df13.rename(columns={'baths':'Baths'}, inplace=True)

    df13['Building Security']=None
    df13['Housing Type']=None

    df13['listingType']=df13['listingType'].where(pd.notnull(df13['listingType']), None)
    df13['listingType']=df13['listingType'].str.replace('sale','Sale')
    df13['listingType']=df13['listingType'].str.replace('rent','Rent')
    df13.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df13['Days on Market']=None
    df13['Number of Guests']=None
    amenities=[]
    for item in df13['amenities']:   
        amenities.append(len(item))
    df13['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df13['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df13['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df13['Occupancy']=None
    df13['Parking']=None
    df13['Lot Area (s.f)']=None
    df13['Fees (commissions, cleaning etc)']=None
    converted_list= list(map(convert_to_sqft, df13['size'].values))
    df13['size']=converted_list
    df13.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df13.rename(columns={'city':'Location: City'}, inplace=True)
    df13.rename(columns={'district':'Location: District'}, inplace=True)
    df13['Location: Lat']=None
    df13['Location: Lon']=None
    df13['Location: Neighbourhood']=None
    df13['Location: Country']='Morocco'
    df13['Location: City']=df13['Location: City'].replace('and',None)
    df13['Location: City']=df13['Location: City'].replace('Property',None)
    df13.rename(columns={'address':'Location: Address'}, inplace=True)
    df13['pricingCriteria']=df13['pricingCriteria'].replace('',None)
    df13['pricingCriteria']=df13['pricingCriteria'].str.replace('per day','Day')
    df13['pricingCriteria']=df13['pricingCriteria'].str.replace('per week','Week')
    df13.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    
    df13['localPrice'] = df13['price']
    df13['localCurrency'] = df13['currency']
    DhToUsd=0.27
    EurToUsd=1.11
    prices=[]
    for idx,item in enumerate(df13['price']):
        if df13['currency'][idx]=='DH':
            item=item*DhToUsd
        elif df13['currency'][idx]=='EUR':
            item=item*DhToUsd        
        prices.append(item)
    df13['price']=prices
    df13.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df13['priceDiff']):
        if df13['currency'][idx]=='DH':
            item=item*DhToUsd
        elif df13['currency'][idx]=='EUR':
            item=item*DhToUsd        
        prices.append(item)
    df13['priceDiff']=prices

    df13.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df13['Price per s.f.']=df13['Price']/df13['Internal Area (s.f)']

    df13['Location: City']=df13['Location: City'].replace('Dar',None)
    df13['Location: City']=df13['Location: City'].str.replace('F%c%as','Fez')
    df13['Location: City']=df13['Location: City'].str.replace('Salé','Sale')
    df13['Location: City']=df13['Location: City'].str.replace('F%c%as','Fez')
    df13['Location: City']=df13['Location: City'].str.replace('\d+', '')
    df13['Location: City']=df13['Location: City'].str.strip()
    df13['Location: City']=df13['Location: City'].str.replace('County','')

    cityGDP = []
    for item in df13['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df13['City GDP per Capita']=cityGDP

    population = []
    for item in df13['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df13['City Population']=population

    populationGrowthRate = []
    for item in df13['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df13['City Population Growth Rate']=populationGrowthRate

    df13.drop(['propertyId','currency'], axis=1,inplace=True)
    df13 = df13.reindex(sorted(df13.columns), axis=1)

    return df13