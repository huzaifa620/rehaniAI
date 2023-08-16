import pandas as pd
import json
from send_data import send_data
from add_lat_long_with_calculations import add_lat_long_with_calculations
from filters.ethiopianProperties_filter import ethiopianProperties_filter
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

    df1 = ethiopianProperties_filter()

    df_concat = pd.concat([df1], ignore_index=True)
    df_concat['Location: City'] = df_concat['Location: City'].str.replace('\d+', '').str.strip().str.replace('County', '')
    df_concat = df_concat.rename(columns=column_dict)
    df_concat['rehaniID'] = df_concat['rehaniID'].astype(str)

    df_concat = add_lat_long_with_calculations(df_concat)
    send_data(df_concat, finalDatabaseName, collectionName)

if __name__ == "__main__":
    main()
