from shapely.geometry import shape, Point
from concurrent.futures import ThreadPoolExecutor
from shapely.validation import make_valid
import os, json
from datetime import datetime
from tqdm import tqdm
import threading, math
from tenacity import retry, stop_after_attempt, wait_fixed

countries = {
    'Ethiopia': 'ETH',
    'Rwanda': 'RWA',
    'Ghana': 'GHA',
    'Kenya': 'KEN',
    'Uganda': 'UGN',
    'Nigeria': 'NIG',
    'Tanzania': 'TZA',
    'Senegal': 'SEN',
    'Egypt': 'EGY',
    'Gambia': 'GAM',
    'Morocco': 'MOR',
    'South Africa': 'SAR',
    'Democratic Republic of the Congo': 'DRC',
    'Zimbabwe': 'ZIM',
    'Namibia': 'NAM',
    'Angola': 'ANG',
    'Mozambique': 'MOZ',
    'Malawi': 'MAL',
    'Zambia': 'ZAM',
    'Ivory Coast': 'CDI',
    'Burundi': 'BUR',
    'South Sudan': 'SSD',
    'Botswana': 'BOT'
}

name2Countries = ('Democratic Republic of the Congo', 'Zambia', 'Zimbabwe', 'Morocco', 'Egypt', 'Botswana', 'Gambia', 'Namibia')
name4Countries = ('Rwanda', 'Uganda')
name2CityCountries = ('Uganda')

thread_local = threading.local()

def process_geojson_feature(feature, point, result):
    try:
        geometry = shape(feature['geometry'])
        
        if not geometry.is_valid:
            geometry = make_valid(geometry)

        if geometry.contains(point):
            properties = feature['properties']
            consolidatedCountry = properties.get('COUNTRY', result['locationCountry'])

            if consolidatedCountry == 'Nigeria':
                consolidatedCity = properties['lganame']
                consolidatedNeighbourhood = properties['wardname']
                consolidatedState = properties['statename']
            elif consolidatedCountry == 'South Africa':
                consolidatedCity = properties['NAME_3']
                consolidatedNeighbourhood = f"{properties['NAME_3']} Ward {properties['NAME_4']}"
                consolidatedState = None
            elif consolidatedCountry == 'Ghana':
                consolidatedCity = properties['NAME_1']
                consolidatedNeighbourhood = properties['NAME_2']
                consolidatedState = None
            else:
                consolidatedCity = properties.get('NAME_2' if result['locationCountry'] in name2CityCountries else 'NAME_1', "")
                consolidatedNeighbourhood = properties.get('NAME_2' if result['locationCountry'] in (name2Countries, name4Countries) else 'NAME_3', "")
                consolidatedState = None
        
            return geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState
        else:
            return None, None, None, None, None
    except Exception as e:
        return None, None, None, None, None

def assignDefaultValues(df_concat, ind):
    location_neighbourhood = df_concat.at[ind, 'locationNeighbourhood']
    location_city = df_concat.at[ind, 'locationCity']
    location_country = df_concat.at[ind, 'locationCountry']

    df_concat.at[ind, 'location'] = ", ".join(filter(None, [location_neighbourhood, location_city, location_country]))
    df_concat.at[ind, 'consolidatedCountry'] = location_country
    df_concat.at[ind, 'consolidatedCity'] = location_city
    df_concat.at[ind, 'consolidatedNeighbourhood'] = location_neighbourhood
    df_concat.at[ind, 'consolidatedState'] = None

def add_more_calculations(df_concat):

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def process_row(ind, result):
        
        df_concat.at[ind, 'lastUpdated'] = datetime.now().date().strftime("%Y-%m-%d")
        try:
            df_concat.at[ind, 'dateAdded'] = datetime.strptime(result['dateAdded'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d") if isinstance(result['dateAdded'], str) else datetime.now().date().strftime("%Y-%m-%d")
        except KeyError:
            df_concat.at[ind, 'dateAdded'] = datetime.now().date().strftime("%Y-%m-%d")
        
        if not hasattr(thread_local, 'mainCountry'):
            thread_local.mainCountry = "Ethiopia"
            current_directory = os.getcwd()
            folder_name = 'shape_files'
            thread_local.folder_path = os.path.join(current_directory, folder_name)
            file_path = os.path.join(thread_local.folder_path, 'ETH.json')
            with open(file_path, 'r', encoding='utf-8') as file:
                thread_local.json_data = json.load(file)

        if (result["locationLat"] or result["locationLon"]):
            if thread_local.mainCountry != result["locationCountry"]:
                thread_local.mainCountry = result["locationCountry"]
                file_path = os.path.join(thread_local.folder_path, f'{countries.get(result["locationCountry"], result["locationCountry"])}.json')
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        thread_local.json_data = json.load(file)
                except FileNotFoundError:
                    assignDefaultValues(df_concat, ind)
                    return

            longitude, latitude = str(result["locationLon"]).replace(',', ''), str(result["locationLat"]).replace(',', '')
            point = Point(longitude, latitude)

            for item in thread_local.json_data.get('features', []):
                geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState = process_geojson_feature(item, point, result)

                if geometry is not None:
                    df_concat.at[ind, 'consolidatedCountry'] = consolidatedCountry
                    df_concat.at[ind, 'consolidatedCity'] = consolidatedCity
                    df_concat.at[ind, 'consolidatedNeighbourhood'] = consolidatedNeighbourhood
                    df_concat.at[ind, 'consolidatedState'] = consolidatedState
                    df_concat.at[ind, 'location'] = f"{consolidatedNeighbourhood}, {consolidatedCity}, {consolidatedCountry}"
                    break
            else:
                assignDefaultValues(df_concat, ind)
            
            return
        
        else:
            assignDefaultValues(df_concat, ind)
            return
        
    # cumulative price
    # last updated
    # if no date listed, use today
    
    total_items = len(df_concat)
    with ThreadPoolExecutor(max_workers=64) as executor1:
        with tqdm(total=total_items, position=0, leave=True, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}/{remaining}]') as progress_bar1:
            futures1 = []

            for ind, result in df_concat.iterrows():
                futures1.append(executor1.submit(process_row, ind, result))

            for future in futures1:
                future.result()
                progress_bar1.set_description(f'Adding neighborhoods by processing latitude/longitude', refresh=True)
                progress_bar1.update(1)

    return df_concat