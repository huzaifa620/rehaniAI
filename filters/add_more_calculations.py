from shapely.geometry import shape, Point
from geopy.distance import geodesic
from concurrent.futures import ThreadPoolExecutor
from shapely.validation import make_valid
import os, json, geopy
from datetime import datetime
from tqdm import tqdm

current_directory = os.getcwd()
folder_name = 'shape_files'
folder_path = os.path.join(current_directory, folder_name)
file_path = os.path.join(folder_path, 'ETH.json')

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

mainCountry = 'Ethiopia'
with open(file_path, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

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

def add_more_calculations(df_concat):

    def process_row(ind, result):
        global folder_path, json_data, mainCountry, countries, name2Countries, name4Countries, name2CityCountries

        if result["locationLat"]:
            if mainCountry != result["locationCountry"]:
                mainCountry = result["locationCountry"]
                file_path = os.path.join(folder_path, f'{countries.get(result["locationCountry"], result["locationCountry"])}.json')
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        json_data = json.load(file)
                except FileNotFoundError:
                    df_concat.at[ind, 'consolidatedCountry'] = df_concat.at[ind, 'locationCountry']
                    df_concat.at[ind, 'consolidatedCity'] = df_concat.at[ind, 'locationCity']
                    df_concat.at[ind, 'consolidatedNeighbourhood'] = df_concat.at[ind, 'locationNeighbourhood']
                    df_concat.at[ind, 'consolidatedState'] = None
                    df_concat.at[ind, 'distanceFromCenter(km)'] = None
                    df_concat.at[ind, 'areaOfPolygon(km²)'] = None
                    df_concat.at[ind, 'neighborhoodDensity(listings/km²)'] = None
                    df_concat.at[ind, 'location'] = f"{df_concat.at[ind, 'locationNeighbourhood']}, {df_concat.at[ind, 'locationCity']}, {df_concat.at[ind, 'locationCountry']}"

            point = Point(result["locationLon"], result["locationLat"])

            for item in json_data.get('features', []):
                geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState = process_geojson_feature(item, point, result)

                if geometry is not None:
                    #print(consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState)
                    polygon_center = geometry.centroid
                    distance_km = geodesic((result["locationLon"], result["locationLat"]), (polygon_center.y, polygon_center.x)).kilometers
                    area_sq_km = geometry.area * 111.32**2
                    neighborhood_density = (len(df_concat) / area_sq_km) if area_sq_km else None

                    df_concat.at[ind, 'consolidatedCountry'] = consolidatedCountry
                    df_concat.at[ind, 'consolidatedCity'] = consolidatedCity
                    df_concat.at[ind, 'consolidatedNeighbourhood'] = consolidatedNeighbourhood
                    df_concat.at[ind, 'consolidatedState'] = consolidatedState
                    df_concat.at[ind, 'distanceFromCenter(km)'] = distance_km
                    df_concat.at[ind, 'areaOfPolygon(km²)'] = area_sq_km
                    df_concat.at[ind, 'neighborhoodDensity(listings/km²)'] = neighborhood_density
                    df_concat.at[ind, 'location'] = f"{consolidatedNeighbourhood}, {consolidatedCity}, {consolidatedCountry}"
                    break
                
        else:

            df_concat.at[ind, 'consolidatedCountry'] = df_concat.at[ind, 'locationCountry']
            df_concat.at[ind, 'consolidatedCity'] = df_concat.at[ind, 'locationCity']
            df_concat.at[ind, 'consolidatedNeighbourhood'] = df_concat.at[ind, 'locationNeighbourhood']
            df_concat.at[ind, 'consolidatedState'] = None
            df_concat.at[ind, 'distanceFromCenter(km)'] = None
            df_concat.at[ind, 'areaOfPolygon(km²)'] = None
            df_concat.at[ind, 'neighborhoodDensity(listings/km²)'] = None
            df_concat.at[ind, 'location'] = f"{df_concat.at[ind, 'locationNeighbourhood']}, {df_concat.at[ind, 'locationCity']}, {df_concat.at[ind, 'locationCountry']}"
    
    def process_row_general(ind, result):
        df_concat.at[ind, 'lastUpdated'] = datetime.now()
        
        listings_within_neighborhood = df_concat[df_concat.apply(
            lambda row: row['consolidatedNeighbourhood'] == result["consolidatedNeighbourhood"],
            axis=1
        )]
        #print(f'{len(listings_within_neighborhood)} neighbor(s) found')
        if not listings_within_neighborhood.empty:
            listings_within_neighborhood = listings_within_neighborhood[listings_within_neighborhood['localPrice'].apply(lambda x: isinstance(x, float))]
            neighborhood_avgPrice = listings_within_neighborhood['localPrice'].mean()
            df_concat.at[ind, 'avgPriceInNeighborhood'] = neighborhood_avgPrice

            # Calculate listing density within a 500m radius
            listings_within_radius = listings_within_neighborhood[listings_within_neighborhood.apply(
                lambda row: geopy.distance.distance(
                    (row['locationLat'], row['locationLon']),
                    (result["locationLat"], result["locationLon"])
                ).m <= 500,
                axis=1
            )]

            if not listings_within_radius.empty:
                listing_density = len(listings_within_radius) / 500

                listings_within_radius = listings_within_radius[listings_within_radius['localPrice'].apply(lambda x: isinstance(x, float))]
                avg_price = listings_within_radius['localPrice'].mean()

                df_concat.at[ind, 'listingDensity(listings/m)'] = listing_density
                df_concat.at[ind, 'avgPriceInCircle'] = avg_price

    total_items = len(df_concat)
    with ThreadPoolExecutor(max_workers=16) as executor:
        
        with tqdm(total=total_items, position=0, leave=True, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}/{remaining}]') as progress_bar1:
            futures = []

            for ind, result in df_concat.iterrows():
                futures.append(executor.submit(process_row, ind, result))

            for future in futures:
                future.result()
                progress_bar1.set_description(f'Adding neighborhoods by processing latitude/longitude', refresh=True)
                progress_bar1.update(1)
        print('')
        with tqdm(total=total_items, position=0, leave=True, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}/{remaining}]') as progress_bar2:
            futures = []

            for ind, result in df_concat.iterrows():
                futures.append(executor.submit(process_row_general, ind, result))

            for future in futures:
                future.result()
                progress_bar2.set_description(f'Finding neighbors and other related info in the processed data', refresh=True)
                progress_bar2.update(1)

        return df_concat