import requests
import zipfile
import io
import pandas as pd
from sqlalchemy import create_engine
import logging
import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class GDELTDataProcessor:
    def __init__(self, database_connection_string: str):
        self.database_connection_string = database_connection_string
        self.latest_data_url = None

    def fetch_latest_data_url(self) -> Optional[str]:
        try:
            response = requests.get(
                "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
            )
            response.raise_for_status()
            lines = response.text.split("\n")
            for line in lines:
                if "export" in line:
                    self.latest_data_url = line.split(" ")[2]
                    logging.info(f"Found data URL: {self.latest_data_url}")
                    return self.latest_data_url
        except requests.RequestException as e:
            logging.error(f"Error fetching the latest data URL: {e}")
        return None

    def download_and_extract_zip_file(self) -> Optional[pd.DataFrame]:
        if not self.latest_data_url:
            logging.error("No data URL provided for download")
            return None

        try:
            response = requests.get(self.latest_data_url)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                extracted_file_name = zip_file.namelist()[0]
                with zip_file.open(extracted_file_name) as extracted_file:
                    data_frame = pd.read_csv(
                        extracted_file, delimiter="\t", header=None
                    )
            return data_frame
        except (
            requests.RequestException,
            zipfile.BadZipFile,
            pd.errors.ParserError,
        ) as e:
            logging.error(f"Error downloading or extracting the ZIP file: {e}")
        return None

    def process_data_frame(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        data_frame.columns = [
            "GlobalEventID",
            "SQLDate",
            "MonthYear",
            "Year",
            "FractionDate",
            "Actor1Code",
            "Actor1Name",
            "Actor1CountryCode",
            "Actor1KnownGroupCode",
            "Actor1EthnicCode",
            "Actor1Religion1Code",
            "Actor1Religion2Code",
            "Actor1Type1Code",
            "Actor1Type2Code",
            "Actor1Type3Code",
            "Actor2Code",
            "Actor2Name",
            "Actor2CountryCode",
            "Actor2KnownGroupCode",
            "Actor2EthnicCode",
            "Actor2Religion1Code",
            "Actor2Religion2Code",
            "Actor2Type1Code",
            "Actor2Type2Code",
            "Actor2Type3Code",
            "IsRootEvent",
            "EventCode",
            "EventBaseCode",
            "EventRootCode",
            "QuadClass",
            "GoldsteinScale",
            "NumMentions",
            "NumSources",
            "NumArticles",
            "AvgTone",
            "Actor1GeoType",
            "Actor1GeoFullName",
            "Actor1GeoCountryCode",
            "Actor1GeoADM1Code",
            "Actor1GeoADM2Code",
            "Actor1GeoLat",
            "Actor1GeoLong",
            "Actor1GeoFeatureID",
            "Actor2GeoType",
            "Actor2GeoFullName",
            "Actor2GeoCountryCode",
            "Actor2GeoADM1Code",
            "Actor2GeoADM2Code",
            "Actor2GeoLat",
            "Actor2GeoLong",
            "Actor2GeoFeatureID",
            "ActionGeoType",
            "ActionGeoFullName",
            "ActionGeoCountryCode",
            "ActionGeoADM1Code",
            "ActionGeoADM2Code",
            "ActionGeoLat",
            "ActionGeoLong",
            "ActionGeoFeatureID",
            "DateAdded",
            "SourceURL",
        ]

        filtered_data_frame = data_frame[
            [
                "GlobalEventID",
                "SQLDate",
                "EventCode",
                "EventBaseCode",
                "EventRootCode",
                "ActionGeoFullName",
                "ActionGeoCountryCode",
                "ActionGeoLat",
                "ActionGeoLong",
                "DateAdded",
                "SourceURL",
            ]
        ]

        us_events_data_frame = filtered_data_frame[
            filtered_data_frame["ActionGeoCountryCode"] == "US"
        ]
        return us_events_data_frame

    def load_data_to_database(self, data_frame: pd.DataFrame) -> None:
        try:
            engine = create_engine(self.database_connection_string)
            with engine.connect() as connection:
                data_frame.to_sql(
                    "events", connection, if_exists="replace", index=False
                )
            logging.info("Data successfully loaded into the database")
        except Exception as e:
            logging.error(f"Error loading data into the database: {e}")

    def run(self) -> None:
        if self.fetch_latest_data_url():
            data_frame = self.download_and_extract_zip_file()
            if data_frame is not None:
                us_events_data_frame = self.process_data_frame(data_frame)
                logging.info(us_events_data_frame)

                self.load_data_to_database(us_events_data_frame)
            else:
                logging.error("Failed to download or extract data")
        else:
            logging.error("No data URL found")


if __name__ == "__main__":
    database_connection_string = os.getenv(
        "DB_CONNECTION_STRING", "postgresql://p_admin:p_admin@localhost:5432/etl"
    )
    processor = GDELTDataProcessor(database_connection_string)
    processor.run()
