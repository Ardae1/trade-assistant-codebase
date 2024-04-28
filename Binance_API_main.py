import pandas as pd
from binance.client import Client
import time
from config import Config
from time_zone_adapter import TimeZoneAdapter
from logs import SingletonLogger


class BinanceConnectionError(Exception):
    pass


class BinanceDataCollector:
    def __init__(self, parity, config_data: Config):
        self.config = config_data
        self.logger = SingletonLogger.get_logger()
        try:
            self.client = Client(
                self.config.BINANCE_API_KEY_PATH, self.config.BINANCE_API_SECRET_PATH
            )
        except Exception as e:
            self.logger.error("Failed to connect to Binance API ", exc_info=True)
            raise BinanceConnectionError("Failed to connect to Binance API")

        self.time_adapter = TimeZoneAdapter()
        self.parity = parity

        if self.client:
            self.logger.info("Binance API Connection succesful..")

    def adjust_dataframe(self, df, coin):
        df = pd.DataFrame(df)
        df = df.drop(df.columns[5:12], axis=1)
        df = df.rename(columns={0: "time", 1: "Open", 2: "High", 3: "low", 4: "close"})
        df["time"] = df["time"].apply(self.time_adapter.adjust_time)
        df["time"] = pd.to_datetime(df["time"])
        df.insert(0, "parity", coin)
        df.set_index("parity", inplace=True)
        return df

    def binance_data_collector(self, pair):
        info_futures = self.client.futures_exchange_info()
        info_spot = self.client.get_exchange_info()
        future_usdt_coinlist = [
            i["symbol"]
            for i in info_futures["symbols"]
            if i["symbol"].endswith("USDT") == True
        ]
        future_btc_coinlist = [
            j["symbol"]
            for j in info_spot["symbols"]
            if j["symbol"].endswith("BTC") == True
        ]

        btc_pairs_base = [pair.replace("BTC", "") for pair in future_btc_coinlist]
        usdt_pairs_base = [pair.replace("USDT", "") for pair in future_usdt_coinlist]
        common_base_currencies = list(set(btc_pairs_base) & set(usdt_pairs_base))

        btc_pairs_final = [
            pair
            for pair in future_btc_coinlist
            if pair.replace("BTC", "") in common_base_currencies
        ]
        usdt_pairs_final = [
            pair
            for pair in future_usdt_coinlist
            if pair.replace("USDT", "") in common_base_currencies
        ]

        intervals = [
            self.client.KLINE_INTERVAL_15MINUTE,
            self.client.KLINE_INTERVAL_1HOUR,
            self.client.KLINE_INTERVAL_4HOUR,
            self.client.KLINE_INTERVAL_1DAY,
            self.client.KLINE_INTERVAL_1WEEK,
        ]

        limit = 1000
        data = {}
        for pair in usdt_pairs_final[:2]:
            for interval in intervals:
                klines = self.client.get_klines(
                    symbol=pair, interval=interval, limit=limit
                )

                data[f"{pair}-{interval}"] = self.adjust_dataframe(klines, pair)
                print(f"{pair}-{interval} executed..")
                time.sleep(0.3)

        for pair in btc_pairs_final[:2]:
            for interval in intervals:
                klines = self.client.get_klines(
                    symbol=pair, interval=interval, limit=limit
                )

                data[f"{pair}-{interval}"] = self.adjust_dataframe(klines, pair)
                print(f"{pair}-{interval} executed..")
                time.sleep(0.3)

        return data

    def get_current_price(self):
        data = self.unique_data_collector()
        return data[f"{self.parity}USDT-15m"].iloc[-1]["close"]

    def get_current_volume(self):
        symbol = f"{self.parity}USDT"
        limit = 1
        interval = self.client.KLINE_INTERVAL_1HOUR
        try:
            klines = self.client.get_klines(
                symbol=symbol, interval=interval, limit=limit
            )
            self.logger.info(
                f"Volume data was succesfully retrieved for coin: {self.parity}"
            )
            return float(klines[0][5])
        except Exception as e:
            error_message = (
                f"An unexpected error occurred while retrieving volume data: {e}"
            )
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def unique_data_collector(self):
        parite_usdt = f"{self.parity}USDT"
        parite_btc = f"{self.parity}BTC"
        if self.parity == "BTC":
            parity_list = [parite_usdt]
        else:
            parity_list = [parite_usdt, parite_btc]

        limit = 700
        intervals = [
            self.client.KLINE_INTERVAL_15MINUTE,
            self.client.KLINE_INTERVAL_1HOUR,
            self.client.KLINE_INTERVAL_4HOUR,
            self.client.KLINE_INTERVAL_1DAY,
            self.client.KLINE_INTERVAL_1WEEK,
        ]
        data = {}
        try:
            for pair in parity_list:
                for interval in intervals:
                    klines = self.client.get_klines(
                        symbol=pair, interval=interval, limit=limit
                    )
                    data[f"{pair}-{interval}"] = self.adjust_dataframe(klines, pair)

            for name in data.keys():
                for col in data[name].columns:
                    if col != "time":
                        data[name][col] = pd.to_numeric(data[name][col])

                if "time" in data[name].columns:
                    data[name]["time"] = pd.to_datetime(data[name]["time"])

            if len(data) > 0:
                self.logger.info(
                    f"Data successfully collected from Binance Client for coin: {self.parity}"
                )
                return data
            else:
                error_message = "No data fetched from Binance Client."
                self.logger.error(error_message)
                raise ValueError(error_message)

        except Exception as e:
            error_message = f"An unexpected error occurred during data collection: {e}"
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def get_coinlist(self):
        info_futures = self.client.futures_exchange_info()
        info_s = self.client.get_exchange_info()

        future_usdt_coinlist = [
            i["symbol"]
            for i in info_futures["symbols"]
            if i["symbol"].endswith("USDT") == True
        ]
        future_btc_coinlist = [
            j["symbol"]
            for j in info_s["symbols"]
            if j["symbol"].endswith("BTC") == True
        ]
        btc_pairs_base = [pair.replace("BTC", "") for pair in future_btc_coinlist]
        usdt_pairs_base = [pair.replace("USDT", "") for pair in future_usdt_coinlist]
        common_base_currencies = list(set(btc_pairs_base) & set(usdt_pairs_base))

        btc_pairs_final = [
            pair
            for pair in future_btc_coinlist
            if pair.replace("BTC", "") in common_base_currencies
        ]
        usdt_pairs_final = [
            pair
            for pair in future_usdt_coinlist
            if pair.replace("USDT", "") in common_base_currencies
        ]
        last_list = [i[:-4] for i in usdt_pairs_final]
        last_list.insert(0, "BTC")

        return last_list
