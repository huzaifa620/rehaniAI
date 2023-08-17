from geopy.geocoders import Nominatim
import json, os
from shapely.geometry import Point, Polygon, MultiPolygon
from geopy.distance import geodesic
from concurrent.futures import ThreadPoolExecutor
import geopy.distance


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
    'South Sudan': 'SSD',
    'Botswana': 'BOT'
}
name2Countries = ('Democratic Republic of the Congo', 'Zambia', 'Zimbabwe', 'Ghana', 'Morocco', 'Egypt', 'Botswana', 'Gambia', 'Namibia')
name4Countries = ('Rwanda', 'Uganda')
name2CityCountries = ('Uganda')

mainCountry = 'Ethiopia'
with open(file_path, 'r', encoding='utf-8') as file:
    json_data = json.load(file)


def add_lat_long_with_calculations(df_concat):
    global json_data, mainCountry, countries, name2Countries, name4Countries, name2CityCountries

    geolocator = Nominatim(user_agent="longLatApp")

    for ind, result in df_concat.iterrows():
        
        if result["locationAddress"] is not None and len(result["locationAddress"].split(" ")) > 1:
            location = geolocator.geocode(result["locationAddress"], timeout=10)
        else:
            if result["locationNeighbourhood"] is None:
                if result["locationDistrict"] is None:
                    if result["locationCity"] is None:
                        if result["locationCountry"] is None:
                            temp = ""
                        else:
                            temp = result["locationCountry"]
                    else:
                        if result["locationCountry"] is None:
                            temp = result["locationCity"]
                        else:
                            temp = result["locationCity"] + ", " + result["locationCountry"]

                else:
                    if result["locationCity"] is None:
                        if result["locationCountry"] is None:
                            temp = result["locationDistrict"]
                        else:
                            temp = result["locationDistrict"] + ", " + result["locationCountry"]
                    else:
                        if result["locationCountry"] is None:
                            temp = result["locationDistrict"] + ", " + result["locationCity"]
                        else:
                            temp = result["locationDistrict"] + ", " + result["locationCity"] + ", " + result["locationCountry"]
            else:
                if result["locationDistrict"] is None:
                    if result["locationCity"] is None:
                        if result["locationCountry"] is None:
                            temp = result["locationNeighbourhood"]
                        else:
                            temp = result["locationNeighbourhood"] + ", " + result["locationCountry"]
                    else:
                        if result["locationCountry"] is None:
                            temp = result["locationNeighbourhood"] + ", " + result["locationCity"]
                        else:
                            temp = result["locationNeighbourhood"] + ", " + result["locationCity"] + ", " + result["locationCountry"]

                else:
                    if result["locationCity"] is None:
                        if result["locationCountry"] is None:
                            temp = result["locationNeighbourhood"] + ", " + result["locationDistrict"]
                        else:
                            temp = result["locationNeighbourhood"] + ", " + result["locationDistrict"] + ", " + result["locationCountry"]
                    else:
                        if result["locationCountry"] is None:
                            temp = result["locationNeighbourhood"] + ", " + result["locationDistrict"] + ", " + result["locationCity"]
                        else:
                            temp = result["locationNeighbourhood"] + ", " + result["locationDistrict"] + ", " + result["locationCity"] + ", " + result["locationCountry"]

            location = geolocator.geocode(temp, timeout=10)

        if location is not None:
            latitude = location.latitude
            longitude = location.longitude
            df_concat.at[ind, 'locationLat'] = latitude
            df_concat.at[ind, 'locationLon'] = longitude
            print(location.latitude, " ----- ", location.longitude)

            if mainCountry != result["locationCountry"]:
                mainCountry = result["locationCountry"]
                json_file_path = f'{countries[result["locationCountry"]]}.json'
                with open(json_file_path, "r") as file:
                    json_data = json.load(file)
            
            point = Point(longitude, latitude)
            for item in json_data['features']:
                if item['geometry'] is not None:
                    coordinates = item['geometry']['coordinates'][0]
                    if item['geometry']['type'] == 'Polygon':
                        multi_polygon = Polygon(coordinates)
                    else:
                        polygons = [Polygon(coords) for coords in coordinates]
                        multi_polygon = MultiPolygon(polygons)
            
                    if multi_polygon.contains(point):
                        if result["locationCountry"] == 'Nigeria':
                            consolidatedCountry = 'Nigeria'
                            consolidatedCity = item['properties']['lganame']
                            consolidatedNeighbourhood = item['properties']['wardname']
                            consolidatedState = item['properties']['statename']

                        elif result["locationCountry"] == 'South Africa':
                            consolidatedCountry = item['properties']['COUNTRY']
                            consolidatedCity = item['properties']['NAME_3']
                            consolidatedNeighbourhood = item['properties']['NAME_3'] + ' Ward ' + item['properties']['NAME_4']
                            consolidatedState = None
                        else:
                            consolidatedCountry = item['properties']['COUNTRY']

                            if result["locationCountry"] in name2CityCountries:
                                consolidatedCity = item['properties']['NAME_2']
                            else:
                                consolidatedCity = item['properties']['NAME_1']

                            if result["locationCountry"] in name2Countries:
                                consolidatedNeighbourhood = item['properties']['NAME_2']
                            elif result["locationCountry"] in name4Countries:
                                consolidatedNeighbourhood = item['properties']['NAME_4']
                            else:
                                consolidatedNeighbourhood = item['properties']['NAME_3']

                            consolidatedState = None

                        print(consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState)
                        polygon_center = multi_polygon.centroid
                        distance = point.distance(polygon_center)
                        distance_km = geodesic((latitude, longitude), (polygon_center.y, polygon_center.x)).kilometers
                        area_sq_km = multi_polygon.area * 111.32**2
                        break

            neighborhood_density = len(df_concat) / area_sq_km
            
            df_concat.at[ind, 'consolidatedCountry'] = consolidatedCountry
            df_concat.at[ind, 'consolidatedCity'] = consolidatedCity
            df_concat.at[ind, 'consolidatedNeighbourhood'] = result["locationNeighbourhood"] if result["locationNeighbourhood"] is not None else consolidatedNeighbourhood
            df_concat.at[ind, 'consolidatedState'] = consolidatedState
            df_concat.at[ind, 'distanceFromCenter(km)'] = distance_km
            df_concat.at[ind, 'areaOfPolygon(km²)'] = area_sq_km
            df_concat.at[ind, 'neighborhoodDensity(listings/km²)'] = neighborhood_density
            df_concat.at[ind, 'location'] = consolidatedNeighbourhood + ', ' + consolidatedCity + ', ' + consolidatedCountry

        else:
            print("Could not geocode address:", result["locationAddress"])
    

    def process_row(ind, result):
        
        listings_within_neighborhood = df_concat[df_concat.apply(
            lambda row: row['consolidatedNeighbourhood'] == result["consolidatedNeighbourhood"],
            axis=1
        )]

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
            future.result()

    return df_concat