import pandas as pd
from prefect import flow, task
import requests
import json
from geojson import Feature, FeatureCollection, Point
from prefect.task_runners import DaskTaskRunner
import os
import time

# task - its prone it failing when env changes
@task
def read_in_data(raw_data_path):
    # Load San Francisco Registered Business Locations
    rbs = pd.read_csv(raw_data_path).iloc[:5]

    return rbs

# Long for loop - iterates through entire df
# Could be a task but might benefit from async
def build_query_string_list(rbs):
    """Building query strings to be sent to API"""
    api_header = "https://nominatim.openstreetmap.org/search?"
    query_tail = "&format=json"

    query_strings = []
    for i, row in rbs.result().iterrows():
        address_part = f"street={row['Street Address']}&city={row['City']}"
        q_string = api_header + address_part + query_tail
        query_strings.append(q_string)

    return query_strings

# I need to have a backup plan for when this fails when given all data
# Task
def get_lat_lon_from_api(query_string):
    """Single call to API, returns lat lon coords in tuple"""
    resp = requests.get(query_string)
    lat = resp.json()[0]['lat']
    lon = resp.json()[0]['lon']

    return (lat, lon)

# I could also make this async
# Flow or Task?
def query_lat_lon_arrays(query_strings):
    """
    Makes an API call for each row of DF
    Returns Lat lon lists in a list
    """
    lat = []
    lon = []
    for string in query_strings:
        lat_lon = get_lat_lon_from_api(string)

        lat.append(lat_lon[0])
        lon.append(lat_lon[1])

    return [lat, lon]

# Task
def insert_lat_lon_cols(df, lat, lon):
    """Appends lat lon lists as columns to DF"""
    df.result().insert(df.result().shape[1], 'lat', lat)
    df.result().insert(df.result().shape[1], 'lon', lon)

    return df.result()

# TODO: I could break this flow into more tasks
# @flow(name="Convert Pandas DF to GeoJSON File",
#       description="Ends with a GeoJSON File that can be thrown into Tableau with fields from the df associated with points/shapes/etc",
#       version=os.getenv("GIT_COMMIT_SHA"))


def fix_dtypes_for_geojson(df, lat_col_title, lon_col_title, datetime_cols):
    df[lat_col_title] = df[lat_col_title].astype(float)
    df[lon_col_title] = df[lon_col_title].astype(float)

    # Convert everything to str
    for col in datetime_cols:
        df[col] = df[col].astype(str)

    return df

def save_to_file(output_geojson_path, feature_collection):
    with open(str(output_geojson_path), 'w', encoding='utf-8') as f:
        json.dump(feature_collection, f, ensure_ascii=False)

# ^Adding this flow to below function results in this error:
# ValueError: [TypeError("'numpy.int64' object is not iterable"),
# TypeError('vars() argument must have __dict__ attribute')]
def convert_df_to_geojson_file(df, lat_col_title, lon_col_title, datetime_cols, output_geojson_path):

    df = fix_dtypes_for_geojson(df, lat_col_title, lon_col_title, datetime_cols)

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

    save_to_file(output_geojson_path, feature_collection)

    print(f"\nGeoJSON Data Saved to {output_geojson_path}.\n")

@flow(task_runner=DaskTaskRunner())
def main(raw_data_path,
         output_geojson_path):

    rbs = read_in_data(raw_data_path)

    query_strings = build_query_string_list(rbs)

    ll = query_lat_lon_arrays(query_strings)
    lat = ll[0]
    lon = ll[1]

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
        output_geojson_path
    )


if __name__ == '__main__':
    start_time = time.time()

    raw_data_path = 'notebooks/raw/Registered_Business_Locations_-_San_Francisco.csv'

    state = main(raw_data_path, 'geojson_0.geojson')

    print(state)

    print("--- %s seconds ---" % (time.time() - start_time))
