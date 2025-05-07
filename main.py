import os
from downloader import FivaPaisaDownloader
from dotenv import load_dotenv

def test_downloader():
    load_dotenv()
    creds = {
        "APP_NAME": os.getenv("APP_NAME"),
        "USER_ID": os.getenv("USER_ID"),
        "APP_SOURCE": os.getenv("APP_SOURCE"),
        "PASSWORD": os.getenv("USER_PASSWORD"),
        "USER_KEY": os.getenv("USER_KEY"),
        "ENCRYPTION_KEY": os.getenv("ENCRYPTION_KEY"),
    }

    five_paisa_downloader = FivaPaisaDownloader.FivePaisaDownloader(creds, os.getenv("TOTP_SECRET"))
    five_paisa_downloader.connect()
    five_paisa_downloader.get_historical_data(
        exchange = 'N',
        exchange_segment = 'c',
        scrip_names = [
            "DRREDDY",
            "RELIANCE",
            "TATAMOTORS",
            "UPL",
            "ICICIPRULI",
            "HAL",
            "RELIGARE",
            "SUNTECK",
            "ONGC",
            "MOTILALOFS",
            "IGPL",
            "HINDPETRO",
            "NIFTY",
            "BANKNIFTY",
        ],
        time_period = '30m',
        from_date = '2020-01-01',
        to_date = '2025-05-01',
    )

if __name__ == '__main__':
    test_downloader()
