import pandas as pd
from prefect import flow, task
import requests
import json
from geojson import Feature, FeatureCollection, Point
from prefect.task_runners import DaskTaskRunner
import os
import time
import asyncio


# task - its prone it failing when env changes
@task(name="Read in DataFrame from CSV")
def read_in_data_t(raw_data_path: str):
    # Load San Francisco Registered Business Locations
    raw_df = pd.read_csv(raw_data_path).iloc[:5]

    return raw_df


# Long for loop - iterates through entire df
# Could be a task but might benefit from async
@task(name="Build API Queue")
def build_api_call_list(address_df: pd.DataFrame):
    """
    Building query strings to be sent to API
    :df: Dataframe with 'Street Address' and 'City Columns'
    """

    api_header = "https://nominatim.openstreetmap.org/search?"
    query_tail = "&format=json"

    query_strings = []
    for i, row in address_df.iterrows():
        address_part = f"street={row['Street Address']}&city={row['City']}"
        q_string = api_header + address_part + query_tail
        query_strings.append(q_string)

    return query_strings


# I need to have a backup plan for when this fails when given all data
# I should be able to add retries ... but really I just want a pass here not a retry
# Or could I create a retry with a modified string?
@task(name="Bullets to API")
def get_lat_lon_from_api(query_string: str):
    """Single call to API, returns lat lon coords in tuple"""
    resp = requests.get(query_string)
    # Failure Encountered
    lat = resp.json()[0]['lat']
    lon = resp.json()[0]['lon']

    return (lat, lon)


# Caching could work here? Or would it make more sense to cache

@flow(name='Machine Gun to API')
def query_lat_lon_arrays(query_strings: list[str]):
    """
    Makes an API call for each row of DF
    Returns Lat lon lists in a list
    """
    lat = []
    lon = []
    for string in query_strings:
        lat_lon = get_lat_lon_from_api(string).result()

        lat.append(lat_lon[0])
        lon.append(lat_lon[1])

    return [lat, lon]


# This one might be a bit tricky
@task(name="Append Lat/Lon Info into New Cols")
def insert_lat_lon_cols(
        df: pd.DataFrame,
        lat: list[float],
        lon: list[float]
):
    """Appends lat lon lists as columns to DF"""

    df.insert(df.shape[1], 'lat', lat)
    df.insert(df.shape[1], 'lon', lon)

    return df


@task(name="Fix Dtypes for GEOJson")
def fix_dtypes_for_geojson(
        df: pd.DataFrame,
        datetime_cols: list = []
):
    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)

    # Convert everything to str
    if datetime_cols:
        for col in datetime_cols:
            df[col] = df[col].astype(str)

    return df


@task(name="Convert DF to GeoJSON")
def convert_df_to_geojson_file(df: pd.DataFrame):
    # This could be its own function maybe?
    # columns used for constructing geojson object
    features = df.apply(
        lambda row: Feature(geometry=Point((float(row['lon']), float(row['lat'])))),
        axis=1).tolist()

    # all the other columns used as properties
    properties = df.drop(['lat', 'lon'], axis=1).to_dict('records')

    # I can probably drop this
    # whole geojson object
    # feature_collection = FeatureCollection(features=features, properties=properties)

    for i in range(len(features)):
        features[i]["properties"] = properties[i]

    feature_collection = FeatureCollection(features=features)

    return feature_collection


@task(name="Save GeoJSON to File")
def save_to_file(
        feature_collection,
        # Could I use flow metat data to increment a default output file no.?
        # Like try_0.geojson, try_1.geojson
        output_geojson_path: str = 'geospatial_data.geojson'
):
    with open(str(output_geojson_path), 'w', encoding='utf-8') as f:
        json.dump(feature_collection, f, ensure_ascii=False)

    print(f"\nGeoJSON Data Saved to {output_geojson_path}.\n")


@flow(name="DF to GeoJSON",
      version=os.getenv("GIT_COMMIT_SHA"))
def main(
        raw_data_path: str,
        output_geojson_path: str,
        datetime_cols: list[str] = []
):
    address_df = read_in_data_t(raw_data_path).result()

    api_calls = build_api_call_list(address_df).result()

    lat, lon = query_lat_lon_arrays(api_calls).result()

    lat_lon_df = insert_lat_lon_cols(address_df, lat, lon).result()

    cleaned_lat_lon_df = fix_dtypes_for_geojson(lat_lon_df, datetime_cols).result()

    feature_collection = convert_df_to_geojson_file(cleaned_lat_lon_df).result()

    save_to_file(feature_collection, output_geojson_path)

    print('------------ MAIN ----- Done! ------------')


if __name__ == '__main__':
    start_time = time.time()

    # Required User Input
    # I could automate the detection of date columns

    raw_data_path = 'notebooks/raw/Registered_Business_Locations_-_San_Francisco.csv'

    date_time_cols = [
        'Business Start Date',
        'Business End Date',
        'Location Start Date',
        'Location End Date'
    ]

    output_geojson_path = 'output_files/geojson_2.geojson'

    state = main(
        raw_data_path,
        output_geojson_path,
        date_time_cols
    )

    print("||--|-STATE-|--||\n", state)

    print("--- %s seconds ---" % (time.time() - start_time))
