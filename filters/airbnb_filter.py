from get_data import get_data
from get_trading_data import cityElem
import pandas as pd

def airbnb_filter():

    databaseName='airbnb'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df10=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df10['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df10['rehaniID']=hashIds  

    df10['Website'] = "airbnb.com"
    df10['localPrice'] = df10['price']
    df10['localCurrency'] = df10['currency']
    df10.rename(columns={'title':'Title'}, inplace=True)
    df10.rename(columns={'superHostLink':'Agent Contact'}, inplace=True)
    df10.rename(columns={'superHostName':'Agent'}, inplace=True)
    df10['Agent Email Address']=None
    df10.rename(columns={'bed':'Beds'}, inplace=True)
    df10.rename(columns={'bath':'Baths'}, inplace=True)
    df10['Building Security']=None
    df10['Fees (commissions, cleaning etc)']=None
    df10.rename(columns={'imgUrl':'imgUrls'}, inplace=True)
    df10.rename(columns={'guest':'Number of Guests'}, inplace=True)
    df10['Occupancy']=(df10['occupancyOne']/31)*100
    amenities=[]
    for item in df10['amenities']:   
        amenities.append(len(item))
    df10['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df10['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df10['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df10['Type (Rent, Sale, Vacation)']='Vacation'
    df10.rename(columns={'price':'Price'}, inplace=True)
    df10.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df10.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df10['priceStatus']=None
    df10['priceChange']=None
    df10['Price per s.f.']=None
    df10['Price Change']=None
    df10['Parking']=None
    df10['Internal Area (s.f)']=None
    df10['Days on Market']=None
    df10['Lot Area (s.f)']=None
    df10.rename(columns={'city':'Location: City'}, inplace=True)
    df10.rename(columns={'location':'Location: Address'}, inplace=True)
    df10['Location: District']=None
    df10['Location: Neighbourhood']=None
    df10.rename(columns={'longitude':'locationLon'}, inplace=True)
    df10.rename(columns={'latitude':'locationLat'}, inplace=True)
    countries=[]
    for i in df10['Location: Address']:
        countries.append(i.split(',')[-1].strip())
    df10['Location: Country']=countries

    cityGDP = []
    for item in df10['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df10['City GDP per Capita']=cityGDP

    population = []
    for item in df10['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df10['City Population']=population

    populationGrowthRate = []
    for item in df10['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df10['City Population Growth Rate']=populationGrowthRate

    df10.drop(["propertyId","ReviewCount","accuracyRating","amenities","bedType","bedroom","cancellationPolicy","checkinRating","cleanlinessRating","communicationRating","currency","daysFree","discountedPrice","guestSatisfactionOverall","instantBook","isSuperhost","locationRating","newEntry","occupancyRate","occupancyOne","occupancyTwo","personCapacity","recentReview","recentReviewDate","recentReviewRating","reviewsPerMonth","roomType","valueRating"], axis=1,inplace=True)

    df10 = df10.reindex(sorted(df10.columns), axis=1)

    return df10