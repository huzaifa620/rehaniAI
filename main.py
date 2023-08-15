import pandas as pd
import json
from get_trading_data import get_trading_data, countryElem, cityElem
from send_data import send_data
from add_lat_long_with_calculations import add_lat_long_with_calculations


finalDatabaseName='rehaniAIData'
collectionName='data'
with open('columns.json', 'r') as json_file:
    column_dict = json.load(json_file)

def main():

    # Get data from database
    countriesDb, citiesDb = get_trading_data()

    ethiopiaDict, rwandaDict, ghanaDict, kenyaDict, ugandaDict, nigeriaDict, tanzaniaDict, senegalDict, egyptDict, gambiaDict, moroccoDict, southAfricaDict, congoDict, zimbabweDict, namibiaDict, angolaDict, mozambiqueDict, malawiDict, zambiaDict, ivoryCoastDict, burundiDict, southSudanDict, botswanaDict, saudiArabiaDict = countriesDb
    
    df_concat = pd.concat([df1, df2, df3], ignore_index=True)
    df_concat['Location: City'] = df_concat['Location: City'].str.replace('\d+', '').str.strip().str.replace('County', '')
    df_concat = df_concat.rename(columns=column_dict)
    df_concat['rehaniID'] = df_concat['rehaniID'].astype(str)

    df_concat = add_lat_long_with_calculations(df_concat)
    send_data(df_concat, finalDatabaseName, collectionName)

if __name__ == "__main__":
    main()
