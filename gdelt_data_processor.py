import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json

# Constants
GDLET_SOURCE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
COUNTIES_GEOJSON_URL = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "etl"
DB_USER = "p_admin"
DB_PASSWORD = "p_admin"

# Event code mappings
event_root_codes = {
    "14": "PROTESTS",
    "18": "ASSAULT",
    "19": "FIGHT",
    "20": "USE UNCONVENTIONAL MASS VIOLENCE",
}

event_base_codes = {
    "140": "Engage in political dissent, not specified below",
    "141": "Demonstrate or rally, not specified below",
    "142": "Conduct hunger strike, not specified below",
    "143": "Conduct strike or boycott, not specified below",
    "144": "Obstruct passage, block, not specified below",
    "145": "Protest violently, riot, not specified below",
    "180": "Use unconventional violence, not specified below",
    "181": "Abduct, hijack, or take hostage",
    "182": "Physically assault, not specified below",
    "183": "Conduct suicide, car, or other non-military bombing, not specified below",
    "184": "Use as human shield",
    "185": "Attempt to assassinate",
    "186": "Assassinate",
    "190": "Use conventional military force, not specified below",
    "191": "Impose blockade, restrict movement",
    "192": "Occupy territory",
    "193": "Fight with small arms and light weapons",
    "194": "Fight with artillery and tanks",
    "195": "Employ aerial weapons, not specified below",
    "196": "Violate ceasefire",
    "200": "Use unconventional mass violence, not specified below",
    "201": "Engage in mass expulsion",
    "202": "Engage in mass killings",
    "203": "Engage in ethnic cleansing",
    "204": "Use weapons of mass destruction, not specified below",
}


# Function to fetch latest GDELT data
def fetch_gdelt_data():
    response = requests.get(GDLET_SOURCE_URL)
    data_url = None

    if response.status_code == 200:
        for line in response.text.split("\n"):
            if "export" in line:
                data_url = line.split(" ")[-1]
                break

    if data_url:
        response_data = requests.get(data_url)
        if response_data.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response_data.content)) as zip_file:
                # Assuming there is a single CSV file in the zip
                csv_filename = zip_file.namelist()[0]
                with zip_file.open(csv_filename) as csv_file:
                    return csv_file.read()

    return None


# Function to process and filter GDELT data
def process_gdelt_data(data):
    df = pd.read_csv(io.BytesIO(data), sep="\t", header=None, encoding="latin-1")

    # Filter US events using the counties geoJSON
    with requests.get(COUNTIES_GEOJSON_URL) as counties_response:
        counties_geojson = json.loads(counties_response.content)
        us_counties_fips = [
            feature["properties"]["STATE"] + feature["properties"]["COUNTY"]
            for feature in counties_geojson["features"]
        ]

        # df = df[
        #     df[53].apply(lambda x: isinstance(x, str) and x[:2] in us_counties_fips)
        # ]
        df = df[
            df[53].apply(
                lambda x: (
                    isinstance(x, str) and (len(x) >= 2 and x[:2] in us_counties_fips)
                    if isinstance(x, str)
                    else False
                )
            )
        ]

        print(df)

        # df = df[df[53].apply(lambda x: x[:2] in us_counties_fips)]
        # print(df)
        # df = df[df[53].astype(str).apply(lambda x: x[:2] in us_counties_fips)]
        # print("=================")
        # print(df[53])

    # Mapping event codes
    df[27] = df[27].map(event_base_codes)
    df[28] = df[28].map(event_root_codes)

    return df


# Function to connect to PostgreSQL and load data
def load_data_to_postgres(df):
    # print(df)
    conn_string = f"host='{DB_HOST}' port='{DB_PORT}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Assuming the table is named gdelt_events
    create_table_query = """
        CREATE TABLE IF NOT EXISTS gdelt_events (
            GLOBALEVENTID BIGINT PRIMARY KEY,
            SQLDATE DATE,
            EventBaseCode TEXT,
            EventRootCode TEXT,
            ActionGeo_FullName TEXT,
            ActionGeo_CountryCode TEXT,
            ActionGeo_Lat FLOAT,
            ActionGeo_Long FLOAT,
            DATEADDED TIMESTAMP,
            SOURCEURL TEXT
        );
    """

    cursor.execute(create_table_query)

    for index, row in df.iterrows():
        insert_query = """
            INSERT INTO gdelt_events (GLOBALEVENTID, SQLDATE, EventBaseCode, EventRootCode,
                                      ActionGeo_FullName, ActionGeo_CountryCode, ActionGeo_Lat,
                                      ActionGeo_Long, DATEADDED, SOURCEURL)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        # print(insert_query)
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()


# Main function to orchestrate the entire process
def main():
    gdelt_data = fetch_gdelt_data()

    if gdelt_data:
        processed_df = process_gdelt_data(gdelt_data)
        # print("========================")
        # print(processed_df)
        # print("========================")

        load_data_to_postgres(processed_df)
        print("Data loaded successfully into PostgreSQL.")
    else:
        print("Failed to fetch GDELT data.")


if __name__ == "__main__":
    main()
