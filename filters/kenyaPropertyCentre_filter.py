from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd
import numpy as np
import json, re
from google_currency import convert

def kenyaPropertyCentre_filter():

    databaseName='kenyaPropertyCentre'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df6=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df6['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df6['RehaniID']=hashIds

    df6['Website']='kenyapropertycentre.com'
    df6.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df6.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df6.rename(columns={'agent':'Agent'}, inplace=True)
    df6['Agent Email Address']=None
    df6.rename(columns={'beds':'Beds'}, inplace=True)
    df6.rename(columns={'baths':'Baths'}, inplace=True)
    df6['Building Security']=None
    df6.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df6['listingType']=df6['listingType'].str.replace('For ','')
    df6['listingType']=df6['listingType'].where(pd.notnull(df6['listingType']), None)
    df6.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    today = np.datetime64('today')
    daysOnMarket=(today - df6['addedOn'].values) / np.timedelta64(1, 'D')
    daysOnMarket=daysOnMarket.astype('int')
    df6['Days on Market']=daysOnMarket
    df6['Occupancy']=None
    df6['Number of Guests']=None  
    df6['Number of amenities']=None
    df6['Number of high end amenities (pool, gym, spa)']=None
    parking=[]
    for item in df6['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df6['Parking']=parking 
    df6['localPrice'] = df6['price']
    df6['localCurrency'] = df6['currency']
    kshToUsd=float(json.loads(convert('kes', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df6['price']):
        if df6['currency'][idx]=='KSh':
            item=item*kshToUsd
        prices.append(item)
    df6['price']=prices
    df6.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df6['priceDiff']):
        if df6['currency'][idx]=='KSh':
            item=item*kshToUsd
        prices.append(item)
    df6['pricingCriteria']=df6['pricingCriteria'].where(pd.notnull(df6['pricingCriteria']), None)
    df6['pricingCriteria']=df6['pricingCriteria'].str.replace('per ','')
    df6['pricingCriteria']=df6['pricingCriteria'].str.replace('calendar ','')
    pricingCriteria=[]
    for item in df6['pricingCriteria']:
        if item==None:
            pricingCriteria.append(None)
        elif str(item)=='nan':
            pricingCriteria.append(None)
        elif item=='':
            pricingCriteria.append(None)
        else:
            pricingCriteria.append(item.capitalize())
    df6['pricingCriteria']=pricingCriteria
    df6.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df6['priceDiff']=prices
    df6.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    internalArea=[]
    for item in df6['size']:
        if item==None:
            internalArea.append(None)
        else:
            if 'sqm' in item:
                internalArea.append(float(''.join(re.findall(r'\d+', item)))* 10.7639)
            else:
                internalArea.append(None)
    df6['size']=internalArea
    df6.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df6['Lot Area (s.f)']=None
    df6['Price per s.f.']=df6['Price']/df6['Internal Area (s.f)']
    df6['Fees (commissions, cleaning etc)']=None
    cities=[]
    for item in df6['address']:
        cities.append(item.split(',')[-1].strip())
    df6['Location: City']=cities
    district=[]
    for item in df6['address']:
        district.append(item.split(',')[-2].strip())
    df6['Location: District']=district    
    df6['Location: Country']='Kenya'
    df6.rename(columns={'address':'Location: Address'}, inplace=True)
    df6['Location: Lat']=None
    df6['Location: Lon']=None
    df6['Location: Neighbourhood']=None

    cityGDP = []
    for item in df6['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df6['City GDP per Capita']=cityGDP

    population = []
    for item in df6['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df6['City Population']=population

    populationGrowthRate = []
    for item in df6['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df6['City Population Growth Rate']=populationGrowthRate

    df6['Country GDP Growth Rate'] = getCalcValue('gdpGrowthRate', df6)
    df6['Country Interest Rates'] = getCalcValue('interestRates', df6)
    df6['Country Mortgage Rates'] = getCalcValue('mortgageRates', df6)
    df6['Country gdp'] = getCalcValue('countryGdp', df6)
    df6['Country gdp per capita'] = getCalcValue('countryGdpPerCapita', df6)
    df6['Fed Funds Rate'] = getCalcValue('fedFundsRate', df6)
    df6['Inflation Rate'] = getCalcValue('inflationRate', df6)
    df6['US Mortgage Rates'] = getCalcValue('usMortgageRates', df6)
    df6['Mortgage rate for 10 years'] = getCalcValue('mortgageRateFor10Years', df6)
    df6['Mortgage rate for 15 years'] = getCalcValue('mortgageRateFor15Years', df6)
    df6['Mortgage rate for 20 years'] = getCalcValue('mortgageRateFor20Years', df6)
    df6['Mortgage rate for 30 years'] = getCalcValue('mortgageRateFor30Years', df6)
    df6['Interbank Rate'] = getCalcValue('interbankRate', df6)
    df6['Country Remittance'] = getCalcValue('countryRemittance', df6)
    df6['Tourist Arrivals'] = getCalcValue('touristArrivals', df6)
    df6['Tourist Dollars (in thousands)'] = getCalcValue('touristDollars', df6)
    df6['Foreign Direct Investment'] = getCalcValue('foreignDirectInvestment', df6)
    df6['Personal Income Tax Rate'] = getCalcValue('personalIncomeTaxRate', df6)
    df6['Construction GDP'] = getCalcValue('constructionGDP', df6)
    df6['Unemployment Rate'] = getCalcValue('unemploymentRate', df6)
    df6['Minimum Wage (USD/month)'] = getCalcValue('minimumWage(USD/month)', df6)
    df6['Government Debt To GDP'] = getCalcValue('governmentDebtToGDP', df6)
    df6['Building Permits'] = getCalcValue('buildingPermits', df6)
    df6['Price To Rent Ratio'] = getCalcValue('priceToRentRatio', df6)

    df6.drop(['propertyId','addedOn','coveredArea','currency','description','lastUpdated','marketStatus','parkingSpaces','toilets','totalArea'], axis=1,inplace=True)
    df6 = df6.reindex(sorted(df6.columns), axis=1)

    return df6