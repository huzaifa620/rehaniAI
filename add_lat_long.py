from geopy.geocoders import Nominatim
import math

geolocator = Nominatim(user_agent="longLatApp")

def geocode_address(result):
    try:
        location_info = []

        if isinstance(result["locationAddress"], str) and len(result["locationAddress"].split(" ")) > 1:
            location_info.append(result["locationAddress"])
        else:
            location_info.extend(filter(lambda x: x is not None and not (isinstance(x, float) and math.isnan(x)), [result.get(field, "") for field in ["locationNeighbourhood", "locationDistrict", "locationCity", "locationCountry"]]))

        temp = ", ".join(filter(None, location_info))
        location = geolocator.geocode(temp.replace("Located in", '').strip(), timeout=10)

        if location:
            return location.latitude, location.longitude
        else:
            print("Could not geocode address:", temp)
            return None, None

    except Exception as e:
        print('Skipping property ...', e)
        return None, None

def add_lat_long(df_concat):
    # Filter the DataFrame to get only rows where locationLat is None
    df_to_process = df_concat[df_concat["locationLat"].isna()]

    for ind, result in df_to_process.iterrows():
        latitude, longitude = geocode_address(result)
        print(latitude, longitude)
        df_concat.at[ind, 'locationLat'] = latitude
        df_concat.at[ind, 'locationLon'] = longitude

    return df_concat
