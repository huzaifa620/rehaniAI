import pandas as pd
import json
from send_data import send_data
from add_lat_long_with_calculations import add_lat_long_with_calculations
from filters.ethiopianProperties_filter import ethiopianProperties_filter
from filters.houseInRwanda_filter import houseInRwanda_filter
from filters.seso_filter import seso_filter
from filters.buyrentkenya_filter import buyrentkenya_filter

import os, sys

# Get the parent directory (project root)
project_root = os.path.dirname(os.path.abspath(__file__))

# Add the project root to the Python path
sys.path.insert(0, project_root)


finalDatabaseName='rehaniAIData'
collectionName='data'
with open('columns.json', 'r') as json_file:
    column_dict = json.load(json_file)

def main():

    df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18, df19 = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None

    df1 = ethiopianProperties_filter()
    df2 = houseInRwanda_filter()
    df3 = seso_filter()
    #df4 = buyrentkenya_filter()

    df_concat = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18, df19], ignore_index=True)
    df_concat['Location: City'] = df_concat['Location: City'].str.replace('\d+', '').str.strip().str.replace('County', '')
    df_concat = df_concat.rename(columns=column_dict)
    df_concat['rehaniID'] = df_concat['rehaniID'].astype(str)

    df_concat = add_lat_long_with_calculations(df_concat)
    send_data(df_concat, finalDatabaseName, collectionName)

if __name__ == "__main__":
    main()
