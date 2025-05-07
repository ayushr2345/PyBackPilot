import os
import time
import pyotp
import sqlite3
import logging
import pandas as pd
from py5paisa import FivePaisaClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

# Load dotenv file
load_dotenv()
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("FivePaisaDownloader.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FivePaisaDownloader:
    def __init__(self, creds: dict, totp_secret: str):
        """
            creds = {
                "APP_NAME"      : "<YOUR_APP_NAME>",
                "USER_ID"       : "<YOUR_USER_ID>",
                "APP_SOURCE"    : "<YOUR_APP_SOURCE>",
                "PASSWORD"      : "<YOUR_PASSWORD>",
                "USER_KEY"      : "<YOUR_USER_KEY>",
                "ENCRYPTION_KEY": "<YOUR_ENCRYPTION_KEY>"
            }
        """
        self.creds = creds
        self.totp = pyotp.TOTP(totp_secret)
        self.client = None
        self.data_path = "data"
        self.scrip_master = "data/scrip_master.db"
        self.time_list = ['1m', '5m', '10m', '15m', '30m', '60m', '1d']
        self.exchange_map = {
            'N': 'NSE',
            'B': 'BSE',
            'M': 'MCX',
            'n': 'NCDEX'
        }
        self.exchange_segment_map = {
            'c': 'Cash',
            'd': 'Derivatives',
            'u': 'Currency_Derivatives',
            'x': 'NCDEX_Commodity',
            'y': 'NSE_BSE_Commodity'
        }

    def generate_totp(self) -> str:
        return self.totp.now()

    def connect(self):
        logger.info("Connecting to 5Paisa...")
        self.client = FivePaisaClient(cred=self.creds)

        for attempt in range(2):  # 2 attempts max: first try + one retry
            try:
                resp = self.client.get_totp_session(
                    os.getenv("CLIENT_ID"),
                    self.generate_totp(),
                    os.getenv("APP_PIN")
                )
                if resp is None:
                    raise Exception("Login response is None")
                logger.info("Logged in to 5Paisa successfully.")
                break  # Successful login, exit loop
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"Login failed, retrying in 30 seconds...\nError: {e}")
                    time.sleep(30)
                else:
                    logger.error("Could not log in to 5Paisa after retry. Please verify your credentials.")
                    raise Exception("Could not log in to 5Paisa after retry. Please verify your credentials.")
        self.check_and_update_scrip_master()

    def check_and_update_scrip_master(self):
        file_path = "data/scrip_master.csv"
        needs_update = True

        if not os.path.exists(self.data_path):
            logger.info(f"Directory {self.data_path} does not exist. Creating it")
            os.makedirs(self.data_path)

        if os.path.exists(file_path):
            modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if datetime.now() - modified_time < timedelta(days=7):
                logger.info("scrip_master.csv is up to date.")
                needs_update = False
            else:
                logger.info("scrip_master.csv is older than 7 days. Updating...")

        if needs_update:
            scrip_data_frame = self.client.get_scrips()
            if scrip_data_frame is None:
                logger.error("Could not fetch Scrip Master")
                raise Exception(f"Could not fetch Scrip Master.")
            scrip_data_frame.to_csv(file_path, index=False)
            logger.info("scrip_master.csv has been updated.")

        self.setup_database()
        self.store_scrip_master_to_db(csv_path=file_path)

    def setup_database(self):
        """
        Sets up the scrip master SQLite database and table.
        Creates the table if it does not already exist
        """
        conn = sqlite3.connect(self.scrip_master)
        cursor = conn.cursor()

        # Create table with only the necessary fields
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS scrip_master
                       (
                           Exch
                           TEXT,
                           ExchType
                           TEXT,
                           ScripCode
                           INTEGER
                           PRIMARY
                           KEY,
                           Name
                           TEXT,
                           Expiry
                           TEXT,
                           StrikeRate
                           REAL,
                           FullName
                           TEXT
                       )
                       ''')

        conn.commit()
        logger.info(f"Database setup completed at {self.scrip_master}")
        return conn

    def store_scrip_master_to_db(self, csv_path: str):
        df = pd.read_csv(csv_path)
        selected_cols = ["Exch", "ExchType", "ScripCode", "Name", "Expiry", "StrikeRate", "FullName"]
        df = df[selected_cols]

        conn = sqlite3.connect(self.scrip_master)
        df.to_sql("scrip_master", conn, if_exists="replace", index=False)
        conn.close()
        logger.info("Scrip master stored to DB successfully.")

    def get_scrip_code_by_name(self, name: str) -> int:
        conn = sqlite3.connect(self.scrip_master)
        cursor = conn.cursor()

        logger.info(f"Searching Scrip code for Scrip name: {name}")
        logger.debug(f"Searching NSE for Scrip name: {name}")
        # 1. Try exact match in NSE
        cursor.execute(
            "SELECT ScripCode FROM scrip_master WHERE Name = ? AND Exch = 'N' COLLATE NOCASE",
            (name,)
        )
        result = cursor.fetchone()

        logger.debug(f"Searching BSE for Scrip name: {name}")
        # 2. If not found, try exact match in BSE
        if not result:
            cursor.execute(
                "SELECT ScripCode FROM scrip_master WHERE Name = ? AND Exch = 'B' COLLATE NOCASE",
                (name,)
            )
            result = cursor.fetchone()

        searched_partially = False
        logger.debug(f"Searching NSE partially for Scrip name: {name}")
        # 3. If still not found, try partial match in NSE
        if not result:
            searched_partially = True
            cursor.execute(
                "SELECT ScripCode FROM scrip_master WHERE Name LIKE ? AND Exch = 'N' COLLATE NOCASE LIMIT 1",
                (f"%{name}%",)
            )
            result = cursor.fetchone()

        logger.debug(f"Searching BSE partially for Scrip name: {name}")
        # 4. If still not found, try partial match in BSE
        if not result:
            searched_partially = True
            cursor.execute(
                "SELECT ScripCode FROM scrip_master WHERE Name LIKE ? AND Exch = 'B' COLLATE NOCASE LIMIT 1",
                (f"%{name}%",)
            )
            result = cursor.fetchone()

        if searched_partially is True:
            logger.info(
                f"For Scrip name: {name}, the scrip code is partially searched with scrip code being: {result[0]}. Please verify that the correct Scrip is fetched!!! ")
        conn.close()
        return result[0] if result else None

    def get_scrip_name_by_code(self, scrip_code: int) -> tuple | None:
        """
        Returns a tuple of (Name, FullName, Exch) for the given ScripCode.
        """
        conn = sqlite3.connect(self.scrip_master)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT Name, FullName, Exch FROM scrip_master WHERE ScripCode = ?",
            (scrip_code,)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            name, full_name, exchange = result
            logger.info(f"ScripCode {scrip_code} corresponds to: {name} ({full_name}), Exchange: {exchange}")
            return result
        else:
            logger.error(f"No scrip found with ScripCode {scrip_code}")
            return None

    def validate_exchange_segment_and_time(self, time_period: str, exchange: str, exchange_segment: str):
        if time_period not in self.time_list:
            logger.error("Invalid Time Frame, it should be within ['1m', '5m', '10m', '15m', '30m', '60m', '1d'].")
            raise Exception("Invalid Time Frame, it should be within ['1m', '5m', '10m', '15m', '30m', '60m', '1d'].")
        if exchange not in self.exchange_map:
            logger.error("Invalid Exchange, it should be within ['N', 'B', 'M', 'n'].")
            raise Exception("Invalid Exchange, it should be within ['N', 'B', 'M', 'n'].")
        if exchange_segment not in self.exchange_segment_map:
            logger.error("Invalid Exchange Segment, it should be within ['c', 'd', 'u', 'x', 'y'].")
            raise Exception("Invalid Exchange Segment, it should be within ['c', 'd', 'u', 'x', 'y'].")

    def get_historical_data(self, exchange: str, exchange_segment: str, scrip_names: list, time_period: str,
                            from_date: str, to_date: str):
        """
            exchange:         "<N (NSE), B (BSE), M (MCX), n (NCDEX)>",
            exchange_segment: "<c (Cash), d (Derivatives), u (Currency Derivatives), x (NCDEX Commodity), y (NSE & BSE Commodity)>",
            scrip_names:      ["", "" <List of scrip names>],
            time_period:      "<1m, 5m, 10m, 15m, 30m, 60m, 1d>",
            from_date:        "<YYYY-MM-DD>",
            to_date:          "<YYYY-MM-DD>"
        """

        if exchange == "" or exchange_segment == "" or len(
                scrip_names) == 0 or time_period == "" or from_date == "" or to_date == "":
            logger.error("Arguments missing. Please provide all the arguments")
            raise Exception("Arguments missing. Please provide all the arguments")
        if self.client is None:
            logger.error("Arguments missing. Please provide all the arguments")
            raise Exception("Not connected. Call connect() first.")
        self.validate_exchange_segment_and_time(time_period, exchange, exchange_segment)

        fetched_scrip_codes = []
        for i in range(0, len(scrip_names)):
            fetched_scrip_code = self.get_scrip_code_by_name(scrip_names[i])
            if fetched_scrip_code is None:
                logger.error(f"No matches found for the scrip name {scrip_names[i]}")
                raise Exception(f"No matches found for the scrip name {scrip_names[i]}")
            fetched_scrip_codes.append(fetched_scrip_code)

        if len(fetched_scrip_codes) != len(scrip_names):
            logger.error("Could not find scrip codes for all the Scrip names provided")
            raise Exception("Could not find scrip codes for all the Scrip names provided")

        scrip_names_corrected = []
        for fetched_scrip_code in fetched_scrip_codes:
            fetched_scrip_name = self.get_scrip_name_by_code(fetched_scrip_code)
            if fetched_scrip_name is None:
                logger.error(f"Could not find scrip name for scrip code: {fetched_scrip_code}")
                raise Exception(f"Could not find scrip name for scrip code: {fetched_scrip_code}")
            scrip_names_corrected.append(fetched_scrip_name[0])

        scrip_codes_map = dict(zip(fetched_scrip_codes, scrip_names_corrected))
        logger.info("Printing Scrip Codes and their corresponding Names:")
        for scrip_code in scrip_codes_map:
            logger.info(f"{scrip_code} : {scrip_codes_map[scrip_code]}")

        exchange_name = self.exchange_map.get(exchange, exchange)
        exchange_segment_name = self.exchange_segment_map.get(exchange_segment, exchange_segment)

        if time_period == '1d':
            for scrip_code in tqdm(scrip_codes_map, desc="Downloading Daily Data"):
                scrip_name = scrip_codes_map[scrip_code]
                logger.debug(
                    f"Fetching data for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")
                file_name = f"data/{scrip_name}/{scrip_name}_{exchange_name}_{exchange_segment_name}_{time_period}_{from_date}_to_{to_date}.csv"
                if os.path.exists(file_name):
                    logger.info(f"Data already exists for {scrip_name} at {file_name}, skipping download.")
                    continue

                data_frame = self.client.historical_data(exchange, exchange_segment, scrip_code, time_period, from_date,
                                                         to_date)
                if data_frame is None:
                    logger.error(
                        f"Could not fetch historical data for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")
                    raise Exception(
                        f"Could not fetch historical data for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")
                if data_frame.empty:
                    logger.info(
                        f"No data found for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")
                    continue

                data_frame['Datetime'] = data_frame['Datetime'].apply(
                    lambda x: datetime.strptime(str(x), "%Y-%m-%dT%H:%M:%S").date())
                self.save_to_csv(data_frame, file_name)
        else:
            self.get_historical_intraday_data(exchange, exchange_segment, scrip_codes_map, time_period, from_date,
                                              to_date)

    def get_historical_intraday_data(self, exchange: str, exchange_segment: str, scrip_codes_map: dict,
                                     time_period: str, from_date: str, to_date: str):
        """
            Exch:            "<N (NSE), B (BSE), M (MCX), n (NCDEX)>",
            ExchangeSegment: "<c (Cash), d (Derivatives), u (Currency Derivatives), x (NCDEX Commodity), y (NSE & BSE Commodity)>",
            scrip_code:       <Scrip-Code from Scrip master>,
            time:            "<1m, 5m, 10m, 15m, 30m, 60m>",
            From:            "<YYYY-MM-DD>",
            To:              "<YYYY-MM-DD>"
        """
        time_list = ['1m', '5m', '10m', '15m', '30m', '60m']
        if time_period not in time_list:
            logger.error("Invalid Time Frame, it should be within ['1m', '5m', '10m', '15m', '30m', '60m'].")
            raise Exception("Invalid Time Frame, it should be within ['1m', '5m', '10m', '15m', '30m', '60m'].")
        if scrip_codes_map is None:
            logger.error("Empty Dict received in get_intraday_historical_data()")
            raise Exception("Empty Dict received in get_intraday_historical_data()")

        exchange_name = self.exchange_map.get(exchange, exchange)
        exchange_segment_name = self.exchange_segment_map.get(exchange_segment, exchange_segment)

        for scrip_code in tqdm(scrip_codes_map, desc="Downloading Intraday Data"):
            scrip_name = scrip_codes_map[scrip_code]
            file_name = f"data/{scrip_name}/{scrip_name}_{exchange_name}_{exchange_segment_name}_{time_period}_{from_date}_to_{to_date}.csv"
            if os.path.exists(file_name):
                logger.info(f"Data already exists for {scrip_name} at {file_name}, skipping download.")
                continue

            logging.debug(
                f"Fetching intraday data for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")

            start_date = datetime.strptime(from_date, "%Y-%m-%d")
            end_date = datetime.strptime(to_date, "%Y-%m-%d")
            data_frames = []
            time_delta = timedelta(days=180)
            current_date = start_date

            while current_date < end_date:
                current_end_date = min(end_date, current_date + time_delta)
                data_frame = self.client.historical_data(exchange, exchange_segment, scrip_code, time_period,
                                                         current_date.strftime("%Y-%m-%d"),
                                                         current_end_date.strftime("%Y-%m-%d"))
                if data_frame is None:
                    logger.error(
                        f"Could not fetch historical data for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {current_date} to {current_end_date}.")
                    raise Exception(
                        f"Could not fetch historical data for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {current_date} to {current_end_date}.")
                if data_frame.empty:
                    logger.info(
                        f"No data found for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {current_date} to {current_end_date}.")
                    current_date = current_end_date + timedelta(1)
                    continue

                data_frames.append(data_frame)
                current_date = current_end_date + timedelta(1)

            if len(data_frames) == 0:
                logger.error(
                    f"No data found for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")
                raise Exception(
                    f"No data found for {scrip_name} belonging to Exchange {exchange_name} and segment {exchange_segment_name} for {time_period} interval from {from_date} to {to_date}.")
            logger.info(
                f"{scrip_name}: as time period is greater than 6 months: {from_date} to {to_date}, downloaded data in chunks of 6 months")
            complete_data_frame = pd.concat(data_frames)

            self.save_to_csv(complete_data_frame, file_name)

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            logger.debug(f"Directory {directory} does not exist. Creating it")
            os.makedirs(directory)

        logger.debug(f"Saving to {filename}...")
        df.to_csv(filename, index=False)
        logger.debug("Saved successfully.")