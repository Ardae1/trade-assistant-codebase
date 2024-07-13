from configs.config import Config
from services.time_zone_adapter import TimeZoneAdapter
import pandas as pd
import ast
from services.Binance_API_main import BinanceDataCollector

from helpers.logs import SingletonLogger


class ExcelParserClass:
    def __init__(self, parity, config: Config, binance_data: BinanceDataCollector):
        self.logger = SingletonLogger.get_logger()
        self.config = config
        self.binance_data = binance_data
        self.file = self.config.get_main_excel_file()
        self.adapted_time = TimeZoneAdapter.convert_to_utc3()
        self.parity = parity
        self.logger.info("Excel Parser Instance initialized..")

    def excel_run_rsi(self, rsi):
        parite_usdt = self.parity + "USDT"
        parite_btc = self.parity + "BTC"
        xl1 = self.file.parse(f"RSI_{rsi}")
        xl1.set_index("parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))

        rsii = {}
        if self.parity == "BTC":
            rsii["usdt_low"] = xl1.loc[parite_usdt]["RSI_Low_Range"]
            rsii["usdt_high"] = xl1.loc[parite_usdt]["RSI_High_Range"]
        else:
            rsii["usdt_low"] = xl1.loc[parite_usdt]["RSI_Low_Range"]
            rsii["btc_low"] = xl1.loc[parite_btc]["RSI_Low_Range"]
            rsii["usdt_high"] = xl1.loc[parite_usdt]["RSI_High_Range"]
            rsii["btc_high"] = xl1.loc[parite_btc]["RSI_High_Range"]

        return rsii

    def excel_run_dailyrange(self):
        try:
            parite_usdt = self.parity + "USDT"
            parite_btc = self.parity + "BTC"
            xl1 = self.file.parse("Coin Daily Candle Range")
            xl1.set_index("Parity", inplace=True)
            for col in xl1.columns:
                xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))

            drange = {}
            drange["usdt"] = xl1.loc[parite_usdt]["Diff"]

            return drange
        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def bear_bullish_calc(self, api_data):

        bear_interval = self.excel_run_bear_bear()["usdt"]
        bull_interval = self.excel_run_bul_bul()["usdt"]
        opening_price = api_data[f"{self.parity}USDT-1h"][
            (api_data[f"{self.parity}USDT-1h"]["time"].dt.day == self.adapted_time.day)
            & (api_data[f"{self.parity}USDT-1h"]["time"].dt.hour == 3)
        ]["Open"].values[0]
        if bear_interval:
            bear_range = [opening_price * (1 - (i / 100)) for i in bear_interval]

        if bull_interval:
            bull_range = [opening_price * (1 + (i / 100)) for i in bull_interval]

        return bear_range, bull_range

    def price_monitor(self, api_data):
        try:
            c_price = api_data[f"{self.parity}USDT-15m"].iloc[-1]["close"]
            daily_range_h, daily_range_l, daily_status, price_range, price_range2 = (
                self.daily_range_calc(api_data)
            )
            bear_range, bull_range = self.bear_bullish_calc(api_data)
            status_list = []

            if daily_range_h:
                if c_price < daily_range_h[0] and c_price > daily_range_h[-1]:

                    status_list.append(
                        (
                            "LOW",
                            "Price has reached to the bearish target price range for daily average interval",
                        )
                    )

                if c_price < daily_range_h[-1]:

                    status_list.append(
                        (
                            "DUMP",
                            "Price has broken down the target price range for daily average interval!",
                        )
                    )

                else:
                    status_list.append(
                        (
                            "NONE",
                            "Daily price is not yet reached to the bearish/bullish target price range for daily average interval",
                        )
                    )

            if daily_range_l:
                if c_price > daily_range_l[0] and c_price < daily_range_l[-1]:

                    status_list.append(
                        (
                            "HIGH",
                            "Price has reached to the bullish target price range for daily average interval",
                        )
                    )

                if c_price >= daily_range_l[-1]:

                    status_list.append(
                        (
                            "SKY",
                            "Price has broken up the target price range for daily average interval!",
                        )
                    )
                else:
                    status_list.append(
                        (
                            "NONE",
                            "Daily price is not yet reached to the bullish/bearish target price range for daily average interval",
                        )
                    )

            if bear_range:
                if c_price < bear_range[0] and c_price > bear_range[-1]:

                    status_list.append(
                        (
                            "BEAR LOW",
                            "Price has moved in the price range for bearish day - bearish range average interval",
                        )
                    )

                if c_price < bear_range[-1]:

                    status_list.append(
                        (
                            "MELT",
                            "Price has broken down the target price range for bearish day - bearish range average interval",
                        )
                    )
                else:
                    status_list.append(
                        (
                            "NONE",
                            "Daily price is not yet reached to the targets for bearish day - bearish range average interval.",
                        )
                    )

            if bull_range:
                if c_price > bull_range[0] and c_price < bull_range[-1]:

                    status_list.append(
                        (
                            "BULL",
                            "Price has moved in the price range for bullish day - bullish range average interval",
                        )
                    )

                if c_price >= bull_range[-1]:

                    status_list.append(
                        (
                            "FLY",
                            "Price has broken up the target price range for bullish day - bullish range average interval",
                        )
                    )
                else:
                    status_list.append(
                        (
                            "NONE",
                            "Daily price is not yet reached to the targets for bullish day - bullish range average interval.",
                        )
                    )

            self.logger.info("Price Monitor was succesfully completed..")
            return (
                status_list,
                daily_status,
                daily_range_h,
                daily_range_l,
                price_range,
                price_range2,
            )

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def excel_run_bul_bear(self):
        parite_usdt = self.parity + "USDT"
        xl1 = self.file.parse("Coin Daily Candle Range")
        xl1.set_index("Parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))

        drange = {}
        drange["usdt"] = xl1.loc[parite_usdt]["BullishDay - Bearish Range"]

        return drange

    def excel_run_bul_bul(self):
        parite_usdt = self.parity + "USDT"
        xl1 = self.file.parse("Coin Daily Candle Range")
        xl1.set_index("Parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))

        drange = {}
        drange["usdt"] = xl1.loc[parite_usdt]["BullishDay - Bullish Range"]

        return drange

    def excel_run_bear_bul(self):
        parite_usdt = self.parity + "USDT"
        xl1 = self.file.parse("Coin Daily Candle Range")
        xl1.set_index("Parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))

        drange = {}
        drange["usdt"] = xl1.loc[parite_usdt]["BearishDay - Bullish Range"]

        return drange

    def excel_run_bear_bear(self):
        parite_usdt = self.parity + "USDT"
        xl1 = self.file.parse("Coin Daily Candle Range")
        xl1.set_index("Parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))

        drange = {}
        drange["usdt"] = xl1.loc[parite_usdt]["BearishDay - Bearish Range"]

        return drange

    def excel_run_percent_down(self, db_name):
        if db_name.endswith("15m"):
            timeline = db_name[-3::]
        else:
            timeline = db_name[-2::]
        xl1 = self.file.parse(f"Fall_percentage_{timeline}")
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: None if x == " " else x)
        return xl1

    def excel_run_percent_up(self, db_name):
        if db_name.endswith("15m"):
            timeline = db_name[-3::]
        else:
            timeline = db_name[-2::]
        xl1 = self.file.parse(f"Rise_percentage_{timeline}")
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: None if x == " " else x)
        return xl1

    def excel_run_volume_data(self):
        xl1 = self.file.parse("Volume_Range")
        xl1.set_index("Parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))
        return xl1.loc[self.parity]

    def volume_analyzer(self, api_data):
        try:
            volume_data = self.excel_run_volume_data()
            current_hour = self.adapted_time.hour
            current_volume = self.binance_data.get_current_volume()
            low_boundary, high_boundary = volume_data[current_hour]
            status_info = []
            status_info.append(
                f"The  cumulative volume range for hour {current_hour}:00 is {low_boundary} - {high_boundary}"
            )
            if low_boundary <= current_volume <= high_boundary:
                status_info.append(
                    f"{low_boundary} <= {current_volume} <= {high_boundary}, we are in actual range!"
                )

            elif current_volume < low_boundary:
                status_info.append(
                    f"experiencing low volume for this hour {current_volume} < {low_boundary}"
                )

            elif current_volume > high_boundary:
                status_info.append(
                    f"experiencing high volume for this hour {current_volume} > {high_boundary}"
                )

            return status_info[0], status_info[1]

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def exce_run_bollinger_data(self):
        if self.parity.endswith("15m"):
            timeline = self.parity[-3::]
        else:
            timeline = self.parity[-2::]
        xl1 = self.file.parse(f"Bollinger_ranges_{timeline}")
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: None if x == " " else x)
        return xl1

    def daily_range_calc(self, api_data):
        try:
            range1 = pd.to_datetime(["02:45", "23:45"]).time

            try:
                opening_price = api_data[f"{self.parity}USDT-1h"][
                    (
                        api_data[f"{self.parity}USDT-1h"]["time"].dt.day
                        == self.adapted_time.day
                    )
                    & (api_data[f"{self.parity}USDT-1h"]["time"].dt.hour == 3)
                ]["Open"].values[0]
            except (KeyError, IndexError) as e:
                error_message = f"Error occurred while extracting opening price: {e}"
                self.logger.error(error_message, exc_info=True)
                raise ValueError(error_message)

            try:
                range_price = api_data[f"{self.parity}USDT-15m"][
                    (
                        api_data[f"{self.parity}USDT-15m"]["time"].dt.day
                        == self.adapted_time.day
                    )
                    & (api_data[f"{self.parity}USDT-15m"]["time"].dt.time >= range1[0])
                    & (api_data[f"{self.parity}USDT-15m"]["time"].dt.time <= range1[1])
                ]
            except (KeyError, IndexError) as e:
                error_message = f"Error occurred while extracting range price: {e}"
                self.logger.error(error_message, exc_info=True)
                raise ValueError(error_message)

            range_max = range_price["High"].max()
            range_min = range_price["low"].min()

            daily_high = None
            daily_low = None

            daily_range_l = None
            daily_range_h = None
            daily_status = []

            latest_data = api_data[f"{self.parity}USDT-15m"].iloc[-1]

            if opening_price < range_max:
                price_range_percent = self.excel_run_bear_bul()["usdt"]
                price_range = [
                    opening_price * (1 + (i / 100)) for i in price_range_percent
                ]
                high_in_range = range_price.loc[
                    (range_price["High"] >= price_range[0])
                    & (range_price["High"] <= price_range[-1])
                ]
                close_in_range = range_price.loc[
                    (range_price["close"] >= price_range[0])
                    & (range_price["close"] <= price_range[-1])
                ]
                low_in_range = range_price.loc[
                    (range_price["low"] >= price_range[0])
                    & (range_price["low"] <= price_range[-1])
                ]

                if high_in_range.empty and close_in_range.empty and low_in_range.empty:

                    daily_status.append(
                        "We are not in a bearish short range for an entry level"
                    )
                    daily_high = range_max
                    daily_range_percent = self.excel_run_dailyrange()["usdt"]
                    daily_range_h = [
                        daily_high * (1 - (i / 100)) for i in daily_range_percent
                    ]

                elif (
                    not high_in_range.empty
                    or not close_in_range.empty
                    or low_in_range.empty
                ):
                    in_range = pd.concat(
                        [high_in_range, close_in_range, low_in_range]
                    ).drop_duplicates()

                    if (
                        (latest_data["close"] >= price_range[0])
                        and (latest_data["close"] <= price_range[-1])
                        and (latest_data["time"] == in_range.iloc[-1]["time"])
                    ):
                        daily_status.append(
                            f"Price is in a bearish short range since {in_range.iloc[0]['time']} for an entry level!"
                        )
                    elif (
                        (latest_data["close"] < price_range[0])
                        or (latest_data["close"] > price_range[0])
                        and (in_range.iloc[0]["time"] != in_range.iloc[-1]["time"])
                    ):
                        daily_status.append(
                            f"Price were in a bearish short range between {in_range.iloc[0]['time']} - {in_range.iloc[-1]['time']} for an entry level"
                        )

                    daily_high = range_max

                    if daily_high:
                        daily_range_percent = self.excel_run_dailyrange()["usdt"]
                        daily_range_h = [
                            daily_high * (1 - (i / 100)) for i in daily_range_percent
                        ]

            if opening_price > range_min:
                price_range_percent2 = self.excel_run_bul_bear()["usdt"]
                price_range2 = [
                    opening_price * (1 - (i / 100)) for i in price_range_percent2
                ]
                low_in_range2 = range_price.loc[
                    (range_price["low"] <= price_range2[0])
                    & (range_price["low"] >= price_range2[-1])
                ]
                close_in_range2 = range_price.loc[
                    (range_price["close"] <= price_range2[0])
                    & (range_price["close"] >= price_range2[-1])
                ]
                high_in_range2 = range_price.loc[
                    (range_price["High"] <= price_range2[0])
                    & (range_price["High"] >= price_range2[-1])
                ]

                if (
                    low_in_range2.empty
                    and close_in_range2.empty
                    and high_in_range2.empty
                ):

                    daily_status.append(
                        "We are not in a bullish long range for an entry level"
                    )
                    daily_low = range_min
                    daily_range_percent2 = self.excel_run_dailyrange()["usdt"]
                    daily_range_l = [
                        daily_low * (1 + (i / 100)) for i in daily_range_percent2
                    ]

                elif (
                    not low_in_range2.empty
                    or not close_in_range2.empty
                    or not high_in_range2.empty
                ):
                    in_range = pd.concat(
                        [low_in_range2, close_in_range2, high_in_range2]
                    ).drop_duplicates()

                    if (
                        (latest_data["close"] <= price_range2[0])
                        and (latest_data["close"] >= price_range2[-1])
                        and (latest_data["time"] == in_range.iloc[-1]["time"])
                    ):
                        daily_status.append(
                            f"Price is in a bullish long range since {in_range.iloc[0]['time']} for an entry level!"
                        )
                    elif (
                        (latest_data["close"] > price_range[0])
                        or (latest_data["close"] < price_range[0])
                        and (in_range.iloc[0]["time"] != in_range.iloc[-1]["time"])
                    ):
                        daily_status.append(
                            f"Price was in a bullish long range between {in_range.iloc[0]['time']} - {in_range.iloc[-1]['time']} for an entry level"
                        )

                    daily_low = range_min

                    if daily_low:
                        daily_range_percent2 = self.excel_run_dailyrange()["usdt"]
                        daily_range_l = [
                            daily_low * (1 + (i / 100)) for i in daily_range_percent2
                        ]

            self.logger.info("Daily range calculation were succesfully completed..")
            return (
                daily_range_h,
                daily_range_l,
                daily_status,
                price_range,
                price_range2,
            )

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)
