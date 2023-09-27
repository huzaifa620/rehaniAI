from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd

def ethiopianProperties_filter():
    databaseName='EthiopianProperties'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df1=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df1['url']:
        hashId=hash(rawId)
        if hashId < 0:
            hashId = int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df1['rehaniID']=hashIds
    df1['Website']='ethiopianproperties.com'
    df1.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df1.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df1.rename(columns={'city':'Location: City'}, inplace=True)
    df1.rename(columns={'neighbourhood':'Location: Neighbourhood'}, inplace=True)

    converted_sizes = []
    for size in df1['size']:
        if size:
            if 'Sq Mt' in size:
                size_in_sq_ft = float(size.replace('Sq Mt', '')) * 10.764
            elif 'Sq Meter'in size:
                size_in_sq_ft = float(size.replace('Sq Meter', '')) * 10.764
            elif 'Sq M' in size:
                size_in_sq_ft = float(size.replace('Sq M', '')) * 10.764
            elif 'Sq KM' in size:
                size_in_sq_ft = float(size.replace('Sq KM', '')) * 1076391
            else:
                size_in_sq_ft = None
        else:
            size_in_sq_ft = None
        converted_sizes.append(size_in_sq_ft)
        
    df1['size']=converted_sizes
    df1.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df1['Type (Rent, Sale, Vacation)'] = df1['Type (Rent, Sale, Vacation)'].str.replace('For','')
    df1['Location: Lat']=None
    df1['Location: Lon']=None
    df1['Location: Country']='Ethiopia'
    df1['Location: Address']=None
    df1['Days on Market']=None
    df1['Lot Area (s.f)']=None
    df1['Housing Type']=None
    df1.rename(columns={'beds':'Beds'}, inplace=True)
    df1.rename(columns={'baths':'Baths'}, inplace=True)
    df1['Parking']=None
    df1['Fees (commissions, cleaning etc)']=None
    df1['Occupancy']=None
    df1['Number of Guests']=None
    df1['localPrice'] = df1['price']
    df1['localCurrency'] = 'USD'

    df1.rename(columns={'price':'Price'}, inplace=True)
    df1.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df1['Price per s.f.']=df1['Price']/df1['Internal Area (s.f)']
    
    amenities=[]
    for item in df1['features']:
        amenities.append(len(item))
    df1['Number of amenities']=amenities
    
    highEndAmenities=[]
    for item in df1['features']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    
    df1['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df1['Building Security']=None

    cityGDP = []
    for item in df1['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df1['City GDP per Capita']=cityGDP

    population = []
    for item in df1['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df1['City Population']=population

    populationGrowthRate = []
    for item in df1['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df1['City Population Growth Rate']=populationGrowthRate

    df1['Agent Email Address']=None
    df1.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df1.rename(columns={'agent':'Agent'}, inplace=True)
    df1.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df1.drop(['amenities','currency','features','garage','propertyId'], axis=1,inplace=True)
    df1['Location: District']=None

    df1 = df1.reindex(sorted(df1.columns), axis=1)

    return df1