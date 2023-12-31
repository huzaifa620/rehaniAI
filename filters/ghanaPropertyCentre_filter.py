from get_data import get_data
from get_trading_data import getCalcValue, cityElem
import pandas as pd
import numpy as np
import json, re
from google_currency import convert

def ghanaPropertyCentre_filter():

    databaseName='ghanaPropertyCentre'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    print(f'Collecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df5=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    print(f'Filtering data of {databaseName}')

    hashIds=[]
    for rawId in df5['url']:
        hashId=hash(rawId)
        if hashId<0:
            hashId=int(str(hashId).replace('-','1'))
        hashIds.append(hashId)
    df5['rehaniID']=hashIds  

    df5['Website']='ghanapropertycentre.com'
    df5.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df5.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df5.rename(columns={'agent':'Agent'}, inplace=True)
    df5['Agent Email Address']=None
    df5.rename(columns={'beds':'Beds'}, inplace=True)
    df5.rename(columns={'baths':'Baths'}, inplace=True)
    df5['Building Security']=None
    df5.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df5['listingType']=df5['listingType'].str.replace('For ','')
    df5['listingType']=df5['listingType'].where(pd.notnull(df5['listingType']), None)
    df5.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    today = np.datetime64('today')
    daysOnMarket=(today - df5['addedOn'].values) / np.timedelta64(1, 'D')
    daysOnMarket=daysOnMarket.astype('int')
    df5['Days on Market']=daysOnMarket
    df5['Occupancy']=None
    df5['Number of Guests']=None
    pricingCriteria=[]
    for item in df5['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df5['Price criteria']=pricingCriteria   
    df5['Number of amenities']=None
    df5['Number of high end amenities (pool, gym, spa)']=None
    parking=[]
    for item in df5['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df5['Parking']=parking 
    df5['localPrice'] = df5['price']
    df5['localCurrency'] = df5['currency']

    gcToUsd=float(json.loads(convert('ghs', 'usd', 1))['amount'])
    prices=[]
    for idx,item in enumerate(df5['price']):
        if df5['currency'][idx]=='GH₵':
            item=item*gcToUsd
        prices.append(item)
    df5['price']=prices
    df5.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df5['priceDiff']):
        if df5['currency'][idx]=='GH₵':
            item=item*gcToUsd
        prices.append(item)
    df5['priceDiff']=prices
    df5.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    internalArea=[]
    for item in df5['size']:
        if item==None:
            internalArea.append(None)
        else:
            if 'sqm' in item:
                internalArea.append(float(''.join(re.findall(r'\d+', item)))* 10.7639)
            else:
                internalArea.append(None)
    df5['size']=internalArea
    df5.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df5['Lot Area (s.f)']=None
    df5['Price per s.f.']=df5['Price']/df5['Internal Area (s.f)']
    df5['Fees (commissions, cleaning etc)']=None
    df5['Location: City']='Accra'
    district=[]
    for item in df5['address']:
        district.append(item.split(',')[-2].strip())
    df5['Location: District']=district    
    df5['Location: Country']='Ghana'
    df5.rename(columns={'address':'Location: Address'}, inplace=True)
    df5['Location: Lat']=None
    df5['Location: Lon']=None
    df5['Location: Neighbourhood']=None

    cityGDP = []
    for item in df5['Location: City'].values:
        try:
            cityGDP.append(cityElem(item)['gdpPerCapita'])
        except (KeyError, TypeError):
            cityGDP.append(None)
    df5['City GDP per Capita']=cityGDP

    population = []
    for item in df5['Location: City'].values:
        try:
            population.append(cityElem(item)['population'])
        except (KeyError, TypeError):
            population.append(None)
    df5['City Population']=population

    populationGrowthRate = []
    for item in df5['Location: City'].values:
        try:
            populationGrowthRate.append(cityElem(item)['populationGrowthRate'])
        except (KeyError, TypeError):
            populationGrowthRate.append(None)
    df5['City Population Growth Rate']=populationGrowthRate

    df5['Country GDP Growth Rate'] = getCalcValue('gdpGrowthRate', df5)
    df5['Country Interest Rates'] = getCalcValue('interestRates', df5)
    df5['Country Mortgage Rates'] = getCalcValue('mortgageRates', df5)
    df5['Country gdp'] = getCalcValue('countryGdp', df5)
    df5['Country gdp per capita'] = getCalcValue('countryGdpPerCapita', df5)
    df5['Fed Funds Rate'] = getCalcValue('fedFundsRate', df5)
    df5['Inflation Rate'] = getCalcValue('inflationRate', df5)
    df5['US Mortgage Rates'] = getCalcValue('usMortgageRates', df5)
    df5['Mortgage rate for 10 years'] = getCalcValue('mortgageRateFor10Years', df5)
    df5['Mortgage rate for 15 years'] = getCalcValue('mortgageRateFor15Years', df5)
    df5['Mortgage rate for 20 years'] = getCalcValue('mortgageRateFor20Years', df5)
    df5['Mortgage rate for 30 years'] = getCalcValue('mortgageRateFor30Years', df5)
    df5['Interbank Rate'] = getCalcValue('interbankRate', df5)
    df5['Country Remittance'] = getCalcValue('countryRemittance', df5)
    df5['Tourist Arrivals'] = getCalcValue('touristArrivals', df5)
    df5['Tourist Dollars (in thousands)'] = getCalcValue('touristDollars', df5)
    df5['Foreign Direct Investment'] = getCalcValue('foreignDirectInvestment', df5)
    df5['Personal Income Tax Rate'] = getCalcValue('personalIncomeTaxRate', df5)
    df5['Construction GDP'] = getCalcValue('constructionGDP', df5)
    df5['Unemployment Rate'] = getCalcValue('unemploymentRate', df5)
    df5['Minimum Wage (USD/month)'] = getCalcValue('minimumWage(USD/month)', df5)
    df5['Government Debt To GDP'] = getCalcValue('governmentDebtToGDP', df5)
    df5['Building Permits'] = getCalcValue('buildingPermits', df5)
    df5['Price To Rent Ratio'] = getCalcValue('priceToRentRatio', df5)

    df5.drop(['propertyId','addedOn','coveredArea','currency','description','lastUpdated','marketStatus','parkingSpaces','toilets','totalArea'], axis=1,inplace=True)
    df5 = df5.reindex(sorted(df5.columns), axis=1)

    return df5