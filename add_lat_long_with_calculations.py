from geopy.geocoders import Nominatim
from shapely.geometry import shape, Polygon, MultiPolygon, Point
from geopy.distance import geodesic
from concurrent.futures import ThreadPoolExecutor
from shapely.validation import make_valid
import os, json, math
from datetime import datetime

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

name2Countries = (
    'Democratic Republic of the Congo', 'Zambia', 'Zimbabwe', 'Ghana', 'Morocco', 'Egypt', 'Botswana', 'Gambia', 'Namibia'
)
name4Countries = ('Rwanda', 'Uganda')
name2CityCountries = ('Uganda')

mainCountry = 'Ethiopia'
with open(file_path, 'r', encoding='utf-8') as file:
    json_data = json.load(file)


def process_geojson_feature(feature, point, result):
    try:
        coordinates = feature.get('geometry', {}).get('coordinates', [])
        # Construct the geometry
        geometry = shape({'type': feature['geometry']['type'], 'coordinates': coordinates})

        # Check if the geometry is valid and fix it if needed
        if not geometry.is_valid:
            geometry = make_valid(geometry)

        # Check if the point is within the geometry
        if geometry.contains(point):
            consolidatedCountry = feature['properties'].get('COUNTRY', result["locationCountry"])
            consolidatedCity = feature['properties'].get('NAME_2' if result["locationCountry"] in name2CityCountries else 'NAME_1', "")
            consolidatedNeighbourhood = feature['properties'].get('NAME_2' if result["locationCountry"] in (name2Countries, name4Countries) else 'NAME_3', "")
            consolidatedState = None
            return geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState
        else:
            return None, None, None, None, None
    except Exception as e:
        print('here', e)
        return None, None, None, None, None


def add_lat_long_with_calculations(df_concat):
    global folder_path, json_data, mainCountry, countries, name2Countries, name4Countries, name2CityCountries

    geolocator = Nominatim(user_agent="longLatApp")

    for ind, result in df_concat.iterrows():
        location_info = []

        if isinstance(result["locationAddress"], str) and len(result["locationAddress"].split(" ")) > 1:
            location_info.append(result["locationAddress"])
        else:
            location_info.extend(filter(lambda x: x is not None and not (isinstance(x, float) and math.isnan(x)), [result.get(field, "") for field in ["locationNeighbourhood", "locationDistrict", "locationCity", "locationCountry"]]))

        temp = ", ".join(filter(None, location_info))
        location = geolocator.geocode(temp, timeout=10)

        if location:
            latitude, longitude = location.latitude, location.longitude
            df_concat.at[ind, 'locationLat'] = latitude
            df_concat.at[ind, 'locationLon'] = longitude
            print(location.latitude, " ----- ", location.longitude, mainCountry, result["locationCountry"])

            if mainCountry != result["locationCountry"]:
                mainCountry = result["locationCountry"]
                file_path = os.path.join(folder_path, f'{countries.get(result["locationCountry"], result["locationCountry"])}.json')
                with open(file_path, "r", encoding="utf-8") as file:
                    json_data = json.load(file)

            point = Point(longitude, latitude)

            for item in json_data.get('features', []):
                geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState = process_geojson_feature(item, point, result)

                if geometry is not None:
                    print(consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState)
                    polygon_center = geometry.centroid
                    distance_km = geodesic((latitude, longitude), (polygon_center.y, polygon_center.x)).kilometers
                    area_sq_km = geometry.area * 111.32**2
                    neighborhood_density = (len(df_concat) / area_sq_km) if area_sq_km else None

                    df_concat.at[ind, 'consolidatedCountry'] = consolidatedCountry
                    df_concat.at[ind, 'consolidatedCity'] = consolidatedCity
                    df_concat.at[ind, 'consolidatedNeighbourhood'] = result.get("locationNeighbourhood", consolidatedNeighbourhood)
                    df_concat.at[ind, 'consolidatedState'] = consolidatedState
                    df_concat.at[ind, 'distanceFromCenter(km)'] = distance_km
                    df_concat.at[ind, 'areaOfPolygon(km²)'] = area_sq_km
                    df_concat.at[ind, 'neighborhoodDensity(listings/km²)'] = neighborhood_density
                    df_concat.at[ind, 'location'] = f"{consolidatedNeighbourhood}, {consolidatedCity}, {consolidatedCountry}"
                    break
        else:
            print("Could not geocode address:", result.get("locationAddress", ""))

    def process_row(ind, result):

        df_concat.at[ind, 'lastUpdated'] = datetime.now()

        listings_within_neighborhood = df_concat[df_concat.apply(
            lambda row: row['consolidatedNeighbourhood'] == result["consolidatedNeighbourhood"],
            axis=1
        )]
        print(f'{len(listings_within_neighborhood)} neighbors founds')
        if not listings_within_neighborhood.empty:
            listings_within_neighborhood = listings_within_neighborhood[listings_within_neighborhood['localPrice'].apply(lambda x: isinstance(x, float))]
            neighborhood_avgPrice = listings_within_neighborhood['localPrice'].mean()
            df_concat.at[ind, 'avgPriceInNeighborhood'] = neighborhood_avgPrice

            # Calculate listing density within a 500m radius
            listings_within_radius = listings_within_neighborhood[listings_within_neighborhood.apply(
                lambda row: geopy.distance.distance(
                    (row['locationLat'], row['locationLon']),
                    (latitude, longitude)
                ).m <= 500,
                axis=1
            )]

            if not listings_within_radius.empty:
                listing_density = len(listings_within_radius) / 500

                listings_within_radius = listings_within_radius[listings_within_radius['localPrice'].apply(lambda x: isinstance(x, float))]
                avg_price = listings_within_radius['localPrice'].mean()

                df_concat.at[ind, 'listingDensity(listings/m)'] = listing_density
                df_concat.at[ind, 'avgPriceInCircle'] = avg_price

    max_workers = 16
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, ind, result) for ind, result in df_concat.iterrows()]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                print("An Error occurred:", e)

    return df_concat