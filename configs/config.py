import yaml
import pandas as pd
from helpers.logs import SingletonLogger


class Config:

    def __init__(self):
        self.logger = SingletonLogger.get_logger()
        self.config_data = self.load_config()
        self.TELEGRAM_TOKEN_PATH = self.config_data["telegram"]["token"]
        self.TELEGRAM_BOT_URL_PATH = self.config_data["telegram"]["bot_url"]
        self.TELEGRAM_CHAT_ID_PATH = self.config_data["telegram"]["chat_ids"]
        self.GITHUB_TOKEN_PATH = self.config_data["github"]["token"]
        self.GITHUB_URL_PATH = self.config_data["github"]["url"]
        self.GITHUB_REPO_OWNER_PATH = self.config_data["github"]["repo_owner"]
        self.GITHUB_REPO_NAME_PATH = self.config_data["github"]["repo_name"]
        self.BINANCE_API_KEY_PATH = self.config_data["binance"]["api_key"]
        self.BINANCE_API_SECRET_PATH = self.config_data["binance"]["api_secret"]
        self.BINANCE_API_FUTURE_URL = self.config_data["binance"]["futures_api_url"]
        self.BINANCE_API_SPOT_URL = self.config_data["binance"]["spot_api_url"]
        self.COINLIST_PATH = self.config_data["coinlist"]["w_parity"]

    def load_config(self):
        with open("config.yaml", "r") as file:
            self.logger.info("reading config file..")
            config = yaml.safe_load(file)
        return config

    def get_excel_data(self, key):
        file_path = self.config_data["excel"][key]
        if file_path:
            return self.load_excel_file(file_path)
        else:
            raise ValueError(f"No file path found for {key} in the config.")

    def load_excel_file(self, file_path):
        if file_path.endswith(".xlsx"):
            return pd.ExcelFile(file_path)
        elif file_path.endswith(".csv"):
            return pd.read_csv(file_path, parse_dates=True)
        else:
            raise ValueError(
                "Invalid file format. Supported formats are .xlsx and .csv."
            )

    def get_file_path(self, key):
        file_path = self.config_data["coinlist"][key]
        if file_path:
            return pd.read_csv(file_path)
        else:
            raise ValueError(f"No file path found for {key} in the config.")

    def get_main_excel_file(self):
        return self.get_excel_data("time-based")

    def get_15m_usdt_excel_file(self):
        return self.get_excel_data("15m_usdt")

    def get_1h_usdt_excel_file(self):
        return self.get_excel_data("1h_usdt")

    def get_4h_usdt_excel_file(self):
        return self.get_excel_data("4h_usdt")

    def get_1d_usdt_excel_file(self):
        return self.get_excel_data("1d_usdt")

    def get_15m_btc_excel_file(self):
        return self.get_excel_data("15m_btc")

    def get_1h_btc_excel_file(self):
        return self.get_excel_data("1h_btc")

    def get_4h_btc_excel_file(self):
        return self.get_excel_data("4h_btc")

    def get_1d_btc_excel_file(self):
        return self.get_excel_data("1d_btc")

    def get_coin_list_w_parity(self):
        return self.get_file_path("w_parity")
    
    def get_coin_list_wo_parity(self):
        return self.get_file_path("wo_parity")
