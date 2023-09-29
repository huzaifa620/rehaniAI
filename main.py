
import json
import os, sys
import pandas as pd
from send_data import send_data
from add_lat_long_with_calculations import add_lat_long_with_calculations

from filters.ethiopianProperties_filter import ethiopianProperties_filter
from filters.houseInRwanda_filter import houseInRwanda_filter
from filters.seso_filter import seso_filter
from filters.buyrentkenya_filter import buyrentkenya_filter
from filters.ghanaPropertyCentre_filter import ghanaPropertyCentre_filter
from filters.kenyaPropertyCentre_filter import kenyaPropertyCentre_filter
from filters.prophunt_filter import prophunt_filter
from filters.propertypro_co_ke_filter import propertypro_co_ke_filter
from filters.propertypro_co_ug_filter import propertypro_co_ug_filter
from filters.airbnb_filter import airbnb_filter
from filters.lamudi_filter import lamudi_filter
from filters.nigeriaPropertyCentre_filter import nigeriaPropertyCentre_filter
from filters.mubawab_filter import mubawab_filter
from filters.property24_filter import property24_filter
from filters.property24_co_ke_filter import property24_co_ke_filter
from filters.propertypro_co_zw_filter import propertypro_co_zw_filter
from filters.propertypro_ng_filter import propertypro_ng_filter
from filters.real_estate_tanzania_filter import real_estate_tanzania_filter
from filters.booking_filter import booking_filter


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

    # df1 = ethiopianProperties_filter()
    # df2 = houseInRwanda_filter()
    # df3 = seso_filter()
    # df4 = buyrentkenya_filter()
    # df5 = ghanaPropertyCentre_filter()
    # df6 = kenyaPropertyCentre_filter()
    # df7 = prophunt_filter()
    # df8 = propertypro_co_ke_filter()
    # df9 = propertypro_co_ug_filter()
    # df10 = airbnb_filter()
    # df11 = lamudi_filter()
    # df12 = nigeriaPropertyCentre_filter()
    # df13 = mubawab_filter()
    # df14 = property24_filter()
    # df15 = property24_co_ke_filter()
    # df16 = propertypro_co_zw_filter()
    # df17 = propertypro_ng_filter()
    # df18 = real_estate_tanzania_filter()
    df19 = booking_filter()

    df_concat = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18, df19], ignore_index=True)
    df_concat['Location: City'] = df_concat['Location: City'].str.replace('\d+', '').str.strip().str.replace('County', '')
    df_concat = df_concat.rename(columns=column_dict)
    df_concat['rehaniID'] = df_concat['rehaniID'].astype(str)

    df_concat = add_lat_long_with_calculations(df_concat)

    int_columns = df_concat.select_dtypes(include=['int']).columns
    df_concat[int_columns] = df_concat[int_columns].astype(float)
    df_concat = df_concat.round(2)

    send_data(df_concat, finalDatabaseName, collectionName)

if __name__ == "__main__":
    main()
