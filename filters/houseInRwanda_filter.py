from get_data import get_data
from get_trading_data import getCalcValue, countryElem, cityElem
import pandas as pd
import json
from google_currency import convert

def houseInRwanda_filter():
    
    databaseName='HouseInRwanda'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df2=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df2['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df2['RehaniID']=hashIds  
    df2['Website']='houseinrwanda.com'
    df2.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df2.rename(columns={'advertType':'Type (Rent, Sale, Vacation)'}, inplace=True)

    typeOfListing=[]
    for item in df2['Type (Rent, Sale, Vacation)']:
        if item==None:
            typeOfListing.append(None)
        elif'Rent'in item:
            typeOfListing.append('Rent')
        elif item.strip()=='Sale':
            typeOfListing.append('Sale')
        elif item.strip()=='Auction':
            typeOfListing.append('Sale')
    df2['Type (Rent, Sale, Vacation)']=typeOfListing   
    df2['localPrice'] = df2['price']
    df2['localCurrency'] = df2['currency']

    df2.rename(columns={'agentCellPhone':'Agent Contact'}, inplace=True)
    df2.rename(columns={'agentEmailAddress':'Agent Email Address'}, inplace=True)
    df2.rename(columns={'agentName':'Agent'}, inplace=True)
    df2.rename(columns={'beds':'Beds'}, inplace=True)
    df2.rename(columns={'baths':'Baths'}, inplace=True)
    df2.rename(columns={'price':'Price'}, inplace=True)
    df2.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df2['Building Security']=None
    df2['Location: Lat']=None
    df2['Location: Lon']=None
    df2.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df2['Fees (commissions, cleaning etc)']=None
    df2['Occupancy']=None
    df2['Number of Guests']=None

    usdToRwf=float(json.loads(convert('rwf', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df2['Price'].values):
        if item==None:
            prices.append(None)    
        elif item=='Price on request ':
            prices.append(None)
        else:
            if df2['currency'].values[idx]=='Rwf':
                item=item*usdToRwf
            prices.append(item)
    df2['Price']=prices
    def convert_to_sqft(area):
        if area is None:
            return None
        sqm = float(area[:-3])  # Extract numerical value as float
        sqft = sqm * 10.7639  # Convert square meters to square feet
        return round(sqft, 2)
    converted_list= list(map(convert_to_sqft, df2['plotSize'].values))
    df2['plotSize']=converted_list
    df2.rename(columns={'plotSize':'Internal Area (s.f)'}, inplace=True)
    df2['Price per s.f.']=df2['Price']/df2['Internal Area (s.f)']
    amenities=[]
    for idx,item in enumerate(df2['amenities']):
        if df2['furnished'].values[idx]=='Yes':
            amenities.append(len(item)+1)
        else:
            amenities.append(len(item))
    df2['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df2['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df2['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df2['Building Security']=None
    df2['Location: Country']='Rwanda'
    cities=[]
    for item in df2['address'].values:
        if type(item)==str:
            cities.append(item.split(',')[0].strip())
        else:
            cities.append(None)
    district=[]
    for item in df2['address'].values:
        if type(item)==str:
            district.append(item.split(',')[1].strip())
        else:
            district.append(None)
    neigbourhood=[]
    for item in df2['address'].values:
        if type(item)==str:
            neigbourhood.append(item.split(',')[2].strip())
        else:
            neigbourhood.append(None)        
    df2['Location: City']=cities
    df2['Location: District']=district
    df2['Location: Neighbourhood']=neigbourhood
    df2.rename(columns={'address':'Location: Address'}, inplace=True)
    df2['Fees (commissions, cleaning etc)']=None
    df2['Parking']=None
    df2['Days on Market']=None
    df2['Price criteria']=None
    df2['Lot Area (s.f)']=None

    cityGDP = []
    for item in df2['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df2['City GDP per Capita']=cityGDP

    population = []
    for item in df2['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df2['City Population']=population

    populationGrowthRate = []
    for item in df2['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df2['City Population Growth Rate']=populationGrowthRate

    df2['Country GDP Growth Rate'] = getCalcValue('gdpGrowthRate', df2)
    df2['Country Interest Rates'] = getCalcValue('interestRates', df2)
    df2['Country Mortgage Rates'] = getCalcValue('mortgageRates', df2)
    df2['Country gdp'] = getCalcValue('countryGdp', df2)
    df2['Country gdp per capita'] = getCalcValue('countryGdpPerCapita', df2)
    df2['Fed Funds Rate'] = getCalcValue('fedFundsRate', df2)
    df2['Inflation Rate'] = getCalcValue('inflationRate', df2)
    df2['US Mortgage Rates'] = getCalcValue('usMortgageRates', df2)
    df2['Mortgage rate for 10 years'] = getCalcValue('mortgageRateFor10Years', df2)
    df2['Mortgage rate for 15 years'] = getCalcValue('mortgageRateFor15Years', df2)
    df2['Mortgage rate for 20 years'] = getCalcValue('mortgageRateFor20Years', df2)
    df2['Mortgage rate for 30 years'] = getCalcValue('mortgageRateFor30Years', df2)
    df2['Interbank Rate'] = getCalcValue('interbankRate', df2)
    df2['Country Remittance'] = getCalcValue('countryRemittance', df2)
    df2['Tourist Arrivals'] = getCalcValue('touristArrivals', df2)
    df2['Tourist Dollars (in thousands)'] = getCalcValue('touristDollars', df2)
    df2['Foreign Direct Investment'] = getCalcValue('foreignDirectInvestment', df2)
    df2['Personal Income Tax Rate'] = getCalcValue('personalIncomeTaxRate', df2)
    df2['Construction GDP'] = getCalcValue('constructionGDP', df2)
    df2['Unemployment Rate'] = getCalcValue('unemploymentRate', df2)
    df2['Minimum Wage (USD/month)'] = getCalcValue('minimumWage(USD/month)', df2)
    df2['Government Debt To GDP'] = getCalcValue('governmentDebtToGDP', df2)
    df2['Building Permits'] = getCalcValue('buildingPermits', df2)
    df2['Price To Rent Ratio'] = getCalcValue('priceToRentRatio', df2)

    df2.drop(['amenities','currency','description','propertyId','expiryDate','furnished','totalFloors'], axis=1,inplace=True)

    df2 = df2.reindex(sorted(df2.columns), axis=1)

    return df2