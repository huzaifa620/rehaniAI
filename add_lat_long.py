from geopy.geocoders import Nominatim
import math
from tqdm import tqdm

geolocator = Nominatim(user_agent="longLatApp")
address, lat, long = '', '', ''

def geocode_address(result):
    global address, lat, long
    try:
        location_info = []

        if isinstance(result["locationAddress"], str) and len(result["locationAddress"].split(" ")) > 1:
            location_info.append(result["locationAddress"])
        else:
            location_info.extend(filter(lambda x: x is not None and not (isinstance(x, float) and math.isnan(x)), [result.get(field, "") for field in ["locationNeighbourhood", "locationDistrict", "locationCity", "locationCountry"]]))

        temp = ", ".join(filter(None, location_info)).replace("Located in", '').strip()
        location = geolocator.geocode(temp, timeout=10)

        address, lat, long = temp, None, None
        if location:
            lat, long = location.latitude, location.longitude
            return address, location.latitude, location.longitude
        else:
            return address, None, None

    except Exception as e:
        return address, None, None

def add_lat_long(df_concat):
    global address, lat, long

    # Filter the DataFrame to get only rows where locationLat is None
    df_to_process = df_concat[df_concat["locationLat"].isna()]

    total_items = len(df_to_process)
    progress_bar = tqdm(total=total_items, position=0, leave=True, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}/{remaining}]')

    for ind, result in df_to_process.iterrows():
        addr, latitude, longitude = geocode_address(result)
        df_concat.at[ind, 'locationLat'] = latitude
        df_concat.at[ind, 'locationLon'] = longitude

        #print(addr, latitude, longitude)
        # Update the description in the progress bar
        progress_bar.set_description(f'Adding latitude/longitude by processing addresses: {df_concat.at[ind, "locationLat"]}, {df_concat.at[ind, "locationLon"]}', refresh=True)

        # Update the progress bar
        progress_bar.update(1)

    progress_bar.close()

    return df_concat
