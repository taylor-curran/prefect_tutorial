import pandas as pd
from prefect import flow
import requests
import json
from geojson import Feature, FeatureCollection, Point


def read_in_data(raw_data_path):
    # Load San Francisco Registered Business Locations
    rbs = pd.read_csv(raw_data_path).iloc[:5]

    return rbs

def build_query_string_list(rbs):
    api_header = "https://nominatim.openstreetmap.org/search?"
    query_tail = "&format=json"

    query_strings = []
    for i, row in rbs.iterrows():
        address_part = f"street={row['Street Address']}&city={row['City']}"
        q_string = api_header + address_part + query_tail
        query_strings.append(q_string)

    return query_strings

# I need to have a backup plan for when this fails when given all data
def get_lat_lon_from_api(query_string):
    resp = requests.get(query_string)
    lat = resp.json()[0]['lat']
    lon = resp.json()[0]['lon']

    return (lat, lon)

def query_lat_lon_arrays(query_strings):
    lat, lon = [], []
    for string in query_strings:
        lat_lon = get_lat_lon_from_api(string)

        lat.append(lat_lon[0])
        lon.append(lat_lon[1])

    return lat, lon

def insert_lat_lon_cols(df, lat, lon):
    df.insert(df.shape[1], 'lat', lat)
    df.insert(df.shape[1], 'lon', lon)

    return df


def convert_df_to_geojson_file(df, lat_col_title, lon_col_title, datetime_cols, filename):
    df[lat_col_title] = df[lat_col_title].astype(float)
    df[lon_col_title] = df[lon_col_title].astype(float)

    # Convert everything to str
    for col in datetime_cols:
        df[col] = df[col].astype(str)

    # columns used for constructing geojson object
    features = df.apply(
        lambda row: Feature(geometry=Point((float(row[lon_col_title]), float(row[lat_col_title])))),
        axis=1).tolist()

    # all the other columns used as properties
    properties = df.drop([lat_col_title, lon_col_title], axis=1).to_dict('records')

    # whole geojson object
    feature_collection = FeatureCollection(features=features, properties=properties)

    for i in range(len(features)):
        features[i]["properties"] = properties[i]

    feature_collection = FeatureCollection(features=features)
    with open(str(filename), 'w', encoding='utf-8') as f:
        json.dump(feature_collection, f, ensure_ascii=False)

    print(f"\nGeoJSON Data Saved to {filename}.\n")




if __name__ == '__main__':
    # Load San Francisco Registered Business Locations
    raw_data_path = 'notebooks/raw/Registered_Business_Locations_-_San_Francisco.csv'
    rbs = read_in_data(raw_data_path)

    query_strings = build_query_string_list(rbs)

    lat, lon = query_lat_lon_arrays(query_strings)

    rbs_loc = insert_lat_lon_cols(rbs, lat, lon)

    convert_df_to_geojson_file(
        rbs_loc,
        'lat',
        'lon',
        [
            'Business Start Date',
            'Business End Date',
            'Location Start Date',
            'Location End Date'
        ],
        'geojson_0.geojson'
    )

