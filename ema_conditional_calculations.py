import pandas as pd
import numpy as np
from excel_db_main import ExcelParserClass
import math
import logging


class EMACalculationClass:
    def __init__(self, parity, api_data, excel_data: ExcelParserClass):
        self.parity = parity
        self.api_data = api_data
        self.excel_data = excel_data
        logging.info("EMA Calculation Instance initialized..")

    # finds corresponding conditions for each given EMA movements.
    def find_condition(self):
        db_conditions = {}
        for db_name, db in self.api_data.items():
            condition_list = []
            for index, dbasee in db.iterrows():
                if (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_100_ema"] > dbasee["close_200_ema"])
                ):

                    condition_list.append([dbasee["time"], "decent_condition(up)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_100_ema"] < dbasee["close_200_ema"])
                ):

                    condition_list.append([dbasee["time"], "decent_condition(down)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "third_condition(up)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "third_condition(down)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                ) or (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_200_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_third_condition(up)"]
                    )
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                ) or (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_200_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_third_condition(down)"]
                    )
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "second_condition(up)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "second_condition(down)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_second_condition(up)"]
                    )
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_second_condition(down)"]
                    )
                    continue

                elif (
                    (
                        (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                        and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                        and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                        and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    )
                ):

                    condition_list.append([dbasee["time"], "first_condition(up)"])
                    continue

                elif (
                    (
                        (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                        and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                        and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                        and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    )
                ):

                    condition_list.append([dbasee["time"], "first_condition(down)"])
                    continue

                elif ((dbasee["close_5_ema"] > dbasee["close_21_ema"])) or (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_100_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_100_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_first_condition(up)"]
                    )
                    continue

                elif ((dbasee["close_5_ema"] < dbasee["close_21_ema"])) or (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_100_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_100_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_first_condition(down)"]
                    )
                    continue

                else:

                    condition_list.append([dbasee["time"], "Unknown condition"])
                    continue

            df = pd.DataFrame(
                {
                    "time": [data[0] for data in condition_list],
                    "condition": [data[1] for data in condition_list],
                }
            )

            db_conditions[db_name] = df

        return db_conditions

    def find_condition_unique(self, db):
        try:
            condition_list = []
            for index, dbasee in db.iterrows():
                if (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_100_ema"] > dbasee["close_200_ema"])
                ):

                    condition_list.append([dbasee["time"], "decent_condition(up)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_100_ema"] < dbasee["close_200_ema"])
                ):

                    condition_list.append([dbasee["time"], "decent_condition(down)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] > dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "third_condition(up)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_60_ema"] < dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "third_condition(down)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                ) or (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_200_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_third_condition(up)"]
                    )
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                ) or (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_200_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_third_condition(down)"]
                    )
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "second_condition(up)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
                ):

                    condition_list.append([dbasee["time"], "second_condition(down)"])
                    continue

                elif (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_second_condition(up)"]
                    )
                    continue

                elif (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                    and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_second_condition(down)"]
                    )
                    continue

                elif (
                    (
                        (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                        and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                        and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
                        and (dbasee["close_5_ema"] > dbasee["close_200_ema"])
                        and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                    )
                ):

                    condition_list.append([dbasee["time"], "first_condition(up)"])
                    continue

                elif (
                    (
                        (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                        and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                        and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    )
                    or (
                        (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                        and (dbasee["close_5_ema"] < dbasee["close_200_ema"])
                        and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
                    )
                ):

                    condition_list.append([dbasee["time"], "first_condition(down)"])
                    continue

                elif ((dbasee["close_5_ema"] > dbasee["close_21_ema"])) or (
                    (dbasee["close_5_ema"] > dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_first_condition(up)"]
                    )
                    continue

                elif ((dbasee["close_5_ema"] < dbasee["close_21_ema"])) or (
                    (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                    and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                ):

                    condition_list.append(
                        [dbasee["time"], "before_first_condition(down)"]
                    )
                    continue

                else:

                    condition_list.append([dbasee["time"], "Unknown condition"])
                    continue

            df = pd.DataFrame(
                {
                    "time": [data[0] for data in condition_list],
                    "condition": [data[1] for data in condition_list],
                }
            )

            df.set_index("time", inplace=True)
            logging.info("Unique EMA Conditions were scanned and collected..")
            return df

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            logging.error(error_message, exc_info=True)
            raise ValueError(error_message)

    # combines RSI ranges for a given coin and counts candles after ema21 crossdown ()
    def count_candles_s(self, rsi_freq):
        try:
            ema21_down_dict = {}
            for db_name, db in self.api_data.items():
                ema21_down = []
                rsi_levels = self.excel_data.excel_run_rsi(14)
                if db_name[-6:-3] == "BTC":
                    rsi_low = rsi_levels["btc_low"]
                else:
                    rsi_low = rsi_levels["usdt_low"]
                first_tag = 6
                second_tag = 13
                fourth_tag = 17
                db.reset_index(drop=True, inplace=True)
                crossing_points = db[
                    (db["close"] < db["close_21_ema"].shift(1))
                    & (db["close"].shift(1) >= db["close_21_ema"])
                ].index.to_list()
                rsi_sma_50 = db[db[f"rsi_{rsi_freq}_sma"] >= 50]
                range1 = "range1"
                range2 = "range2"
                range3 = "range3"

                for i in crossing_points:
                    remaining_rows = len(db) - i

                    if remaining_rows < 40:
                        unique_dataset = db.iloc[i + 1 :]

                    else:
                        unique_dataset = db.iloc[i + 1 : i + 41]

                    unique_time_row = db.iloc[i]

                    if unique_time_row["time"] in rsi_sma_50["time"].values:
                        for k in range(1, len(unique_dataset)):

                            rsi_range3 = np.logical_and(
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] > 23,
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] < 30,
                            )
                            rsi_range2 = np.logical_and(
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] > 39.7,
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] < 50,
                            )
                            rsi_range = np.logical_and(
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] > rsi_low[0],
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] < rsi_low[1],
                            )
                            rsi_condition = (
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}_sma"] >= 50
                            )
                            prev_candles = (
                                unique_dataset.iloc[:k]["close"]
                                < unique_dataset.iloc[:k][f"close_21_ema"]
                            )

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                & rsi_range2
                                and k > 1
                                and k <= 8
                                and rsi_condition
                                and prev_candles.all()
                            ):  # 2-8 arası 40-50

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "ema above 50",
                                        k,
                                        range2,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                & rsi_range
                                and k > 1
                                and k <= 8
                                and rsi_condition
                                and prev_candles.all()
                            ):  # 2-8 arası #range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "ema above 50",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                & rsi_range3
                                and k > 1
                                and k <= 8
                                and rsi_condition
                                and prev_candles.all()
                            ):  # 2-8 #23-30

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "ema above 50",
                                        k,
                                        range3,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                & rsi_range
                                and k > second_tag
                                and k <= fourth_tag
                                and prev_candles.all()
                            ):  # 13-17 arası #range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "ema below 50",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                & rsi_range
                                and k > 1
                                and k <= second_tag
                                and prev_candles.all()
                            ):  # 8-13 arası #range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "ema below 50",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                & rsi_range3
                                and k > 1
                                and k <= 8
                                and prev_candles.all()
                            ):  # 2-8 #23-30

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "ema below 50",
                                        k,
                                        range3,
                                    )
                                )
                                continue

                    else:
                        for k in range(1, len(unique_dataset)):

                            rsi_range3 = np.logical_and(
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] > 23,
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] < 30,
                            )
                            rsi_range2 = np.logical_and(
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] > 39.7,
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] < 50,
                            )
                            rsi_range = np.logical_and(
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] > rsi_low[0],
                                unique_dataset.iloc[k][f"rsi_{rsi_freq}"] < rsi_low[1],
                            )
                            prev_candles = (
                                unique_dataset.iloc[:k]["close"]
                                < unique_dataset.iloc[:k][f"close_21_ema"]
                            )

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k][f"close_21_ema"]
                                )
                                and rsi_range
                                and k > 25
                                and prev_candles.all()
                            ):  # 25+ # range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level4",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k][f"close_21_ema"]
                                )
                                and rsi_range
                                and k > second_tag
                                and k <= fourth_tag
                                and prev_candles.all()
                            ):  # 13-17 #range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level3",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                and rsi_range2
                                and k > first_tag
                                and k <= second_tag
                                and prev_candles.all()
                            ):  # 6-13 #40-50

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level2",
                                        k,
                                        range2,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                and rsi_range3
                                and k > first_tag
                                and k <= second_tag
                                and prev_candles.all()
                            ):  # 6-13 #23-30

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level2",
                                        k,
                                        range3,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                and rsi_range
                                and k > first_tag
                                and k <= second_tag
                                and prev_candles.all()
                            ):  # 6-13 #range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level2",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                and rsi_range3
                                and k > 1
                                and k <= first_tag
                                and prev_candles.all()
                            ):  # 2-6 23-30

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level1",
                                        k,
                                        range3,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                and rsi_range2
                                and k > 1
                                and k <= first_tag
                                and prev_candles.all()
                            ):  # 2-6 #40-50

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level1",
                                        k,
                                        range2,
                                    )
                                )
                                continue

                            if (
                                (
                                    unique_dataset.iloc[k]["close"]
                                    < unique_dataset.iloc[k]["close_21_ema"]
                                )
                                and rsi_range
                                and k > 1
                                and k <= first_tag
                                and prev_candles.all()
                            ):  # 2-6 #range

                                ema21_down.append(
                                    (
                                        unique_dataset.iloc[k]["time"],
                                        "Level1",
                                        k,
                                        range1,
                                    )
                                )
                                continue

                crossing_points_up = self.crossing_points_up(db)
                date_condition = crossing_points_up.iloc[-1]["time"] < ema21_down[-1][0]

                if date_condition:
                    ema21_down_dict[db_name] = ema21_down[-1]
                else:
                    ema21_down_dict[db_name] = " No EMA 21 Cross down observed.. "

            logging.info("Count candle operations were succesfully completed..")
            return ema21_down_dict

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            logging.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def count_candles_r(self, rsi_freq):
        ema21_up_dict = {}
        for db_name, db in self.api_data.items():
            ema21_up = []
            rsi_levels = self.excel_data.excel_run_rsi(14)
            if db_name[-3:] == "BTC":
                rsi_high = rsi_levels["btc_high"]
            else:
                rsi_high = rsi_levels["usdt_high"]

            first_tag = 6
            second_tag = 13
            fourth_tag = 17
            db.reset_index(drop=True, inplace=True)
            crossing_points = db[
                (db["close"] > db["close_21_ema"].shift(1))
                & (db["close"].shift(1) <= db["close_21_ema"])
            ].index.to_list()
            rsi_sma_50 = db[db[f"rsi_{rsi_freq}_sma"] <= 50]
            range1 = "range1"
            range2 = "range2"
            range3 = "range3"

            for i in crossing_points:
                remaining_rows = len(db) - i

                if remaining_rows < 40:
                    unique_dataset = db.iloc[i + 1 :]
                else:
                    unique_dataset = db.iloc[i + 1 : i + 41]

                unique_time_row = db.iloc[i]

                if unique_time_row["time"] in rsi_sma_50["time"].values:
                    for k in range(1, len(unique_dataset)):
                        rsi_range3 = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}"] >= 73
                            and unique_dataset.iloc[k][f"rsi_{rsi_freq}"] <= 80
                        )
                        rsi_range2 = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}"] >= 50
                            and unique_dataset.iloc[k][f"rsi_{rsi_freq}"] <= 55
                        )
                        rsi_range = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}"] >= rsi_high[1]
                            and unique_dataset.iloc[k][f"rsi_{rsi_freq}"] <= rsi_high[0]
                        )
                        prev_candles = (
                            unique_dataset.iloc[:k]["close"]
                            > unique_dataset.iloc[:k][f"close_21_ema"]
                        )
                        rsi_condition = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}_sma"] <= 50
                        )

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            & rsi_range2
                            and k > 1
                            and k <= 8
                            and rsi_condition
                            and prev_candles.all()
                        ):  # 2-8 arası 40-50

                            ema21_up.append(
                                (
                                    unique_dataset.iloc[k]["time"],
                                    "ema above 50-r",
                                    k,
                                    range2,
                                )
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            & rsi_range
                            and k > 1
                            and k <= 8
                            and rsi_condition
                            and prev_candles.all()
                        ):  # 2-8 arası #range

                            ema21_up.append(
                                (
                                    unique_dataset.iloc[k]["time"],
                                    "ema above 50-r",
                                    k,
                                    range1,
                                )
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            & rsi_range3
                            and k > 1
                            and k <= 8
                            and rsi_condition
                            and prev_candles.all()
                        ):  # 2-8 #23-30

                            ema21_up.append(
                                (
                                    unique_dataset.iloc[k]["time"],
                                    "ema above 50-r",
                                    k,
                                    range3,
                                )
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            & rsi_range2
                            and k > second_tag
                            and k <= fourth_tag
                            and prev_candles.all()
                        ):  # 13-17 arası 40-50

                            ema21_up.append(
                                (
                                    unique_dataset.iloc[k]["time"],
                                    "ema below 50-r",
                                    k,
                                    range2,
                                )
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            & rsi_range
                            and k > 1
                            and k <= second_tag
                            and prev_candles.all()
                        ):  # 8-13 arası #range

                            ema21_up.append(
                                (
                                    unique_dataset.iloc[k]["time"],
                                    "ema below 50-r",
                                    k,
                                    range1,
                                )
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            & rsi_range3
                            and k > 1
                            and k <= 8
                            and prev_candles.all()
                        ):  # 2-8 #23-30

                            ema21_up.append(
                                (
                                    unique_dataset.iloc[k]["time"],
                                    "ema below 50-r",
                                    k,
                                    range3,
                                )
                            )
                            continue
                else:
                    for k in range(1, len(unique_dataset)):
                        rsi_range3 = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}"] >= 73
                            and unique_dataset.iloc[k][f"rsi_{rsi_freq}"] <= 80
                        )
                        rsi_range2 = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}"] >= 50
                            and unique_dataset.iloc[k][f"rsi_{rsi_freq}"] <= 55
                        )
                        rsi_range = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}"] >= rsi_high[1]
                            and unique_dataset.iloc[k][f"rsi_{rsi_freq}"] <= rsi_high[0]
                        )
                        prev_candles = (
                            unique_dataset.iloc[:k]["close"]
                            > unique_dataset.iloc[:k][f"close_21_ema"]
                        )

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k][f"close_21_ema"]
                            )
                            and rsi_range
                            and k > 25
                            and prev_candles.all()
                        ):  # 25+ # range

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level4-r", k, range1)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k][f"close_21_ema"]
                            )
                            and rsi_range
                            and k > second_tag
                            and k <= fourth_tag
                            and prev_candles.all()
                        ):  # 13-17 #range

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level3-r", k, range1)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            and rsi_range2
                            and k > first_tag
                            and k <= second_tag
                            and prev_candles.all()
                        ):  # 6-13 #40-50

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level2-r", k, range2)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            and rsi_range3
                            and k > first_tag
                            and k <= second_tag
                            and prev_candles.all()
                        ):  # 6-13 #23-30

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level2-r", k, range3)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            and rsi_range
                            and k > first_tag
                            and k <= second_tag
                            and prev_candles.all()
                        ):  # 6-13 #range

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level2-r", k, range1)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            and rsi_range3
                            and k > 1
                            and k <= first_tag
                            and prev_candles.all()
                        ):  # 2-6 23-30

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level1-r", k, range3)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            and rsi_range2
                            and k > 1
                            and k <= first_tag
                            and prev_candles.all()
                        ):  # 2-6 #40-50

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level1-r", k, range2)
                            )
                            continue

                        if (
                            (
                                unique_dataset.iloc[k]["close"]
                                > unique_dataset.iloc[k]["close_21_ema"]
                            )
                            and rsi_range
                            and k > 1
                            and k <= first_tag
                            and prev_candles.all()
                        ):  # 2-6 #range

                            ema21_up.append(
                                (unique_dataset.iloc[k]["time"], "Level1-r", k, range1)
                            )
                            continue

            crossing_points_down = self.crossing_points_down(db)
            date_condition = crossing_points_down.iloc[-1]["time"] < ema21_up[-1][0]

            if date_condition:
                ema21_up_dict[db_name] = ema21_up[-1]
            else:
                ema21_up_dict[db_name] = " No EMA 21 Cross up observed. "

        return ema21_up_dict

    # finds percentage-based possible fall points - ema21 crossdown onwards.
    def crossing_points_up(self, data):
        # Find the crossing points where the price crosses above the EMA 21
        crossing_points = data[
            (data["close"] > data["close_21_ema"].shift(1))
            & (data["close"].shift(1) <= data["close_21_ema"])
        ]
        return crossing_points

    def crossing_points_down(self, data):
        # Find the crossing points where the price crosses below the EMA 21
        crossing_points = data[
            (data["close"] < data["close_21_ema"].shift(1))
            & (data["close"].shift(1) >= data["close_21_ema"])
        ]
        return crossing_points

    def support_finder(self, condition):
        nonetype_dict = {
            "1w": [],
            "1d": [],
            "4h": [
                "before_second_condition(down)",
                "before_third_condition(down)",
                "first_condition(down)",
            ],
            "1h": [],
            "5m": [],
        }
        try:
            if self.parity != "BTC":
                desired_db = [
                    f"{self.parity}USDT-15m",
                    f"{self.parity}USDT-1h",
                    f"{self.parity}USDT-4h",
                    f"{self.parity}USDT-1d",
                    f"{self.parity}BTC-15m",
                    f"{self.parity}BTC-1h",
                    f"{self.parity}BTC-4h",
                    f"{self.parity}BTC-1d",
                ]
            else:
                desired_db = [
                    f"{self.parity}USDT-15m",
                    f"{self.parity}USDT-1h",
                    f"{self.parity}USDT-4h",
                    f"{self.parity}USDT-1d",
                ]

            desiredlist = {
                desired_db[i]: self.api_data[desired_db[i]]
                for i in range(len(desired_db))
            }
            desired_dict = {}
            for db_name, db in desiredlist.items():
                db.reset_index(drop=True, inplace=True)
                crossing_points2 = db[
                    (db["close"] < db["close_21_ema"].shift(1))
                    & (db["close"].shift(1) >= db["close_21_ema"])
                ]
                c_points_up = self.crossing_points_up(db)
                ema_conditions = self.find_condition_unique(crossing_points2)
                crossing_points = crossing_points2.index.tolist()
                fall_percent = self.excel_data.excel_run_percent_down(db_name)
                df_dict = {}

                for i in crossing_points:
                    if i >= 51:
                        prev_dataset = db.iloc[i - 51 : i]
                        ath = prev_dataset.loc[
                            prev_dataset["High"] == prev_dataset["High"].max(), "High"
                        ].values[0]
                        timee = db.at[i, "time"]
                        condition_stat = ema_conditions.at[timee, "condition"]
                        if any(
                            item == condition_stat
                            for item in nonetype_dict[db_name[-2:]]
                        ):
                            continue
                        else:
                            for condition, c_values in fall_percent.items():
                                if condition_stat == condition:
                                    support_list = [
                                        ath - (ath * float(i)) for i in c_values
                                    ]

                        df_dict[f"{timee}"] = support_list

                df = pd.DataFrame(df_dict)
                date_condition = (
                    c_points_up.iloc[-1]["time"] < crossing_points2.iloc[-1]["time"]
                )
                current_price = db.iloc[-1]["close"]

                if (
                    not df.empty
                    and date_condition
                    and not any(
                        item == condition for item in nonetype_dict[db_name[-2:]]
                    )
                ):  # check if the DataFrame is not empty
                    last_data = df[df.columns[-1]].tolist()
                    adjusted_data = [i for i in last_data if not math.isnan(i)]
                    desired_dict[db_name] = [
                        i for i in adjusted_data if current_price > i
                    ]
                    if not desired_dict[db_name]:
                        desired_dict[db_name] = (
                            "Calculated support levels remains above the current price.."
                        )
                else:
                    desired_dict[db_name] = (
                        "support levels are not valid since no cross down is observed.."  # or handle empty DataFrame as you see fit
                    )

            logging.info("Support Levels were succesfully calculated..")
            return desired_dict

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            logging.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def resistance_finder(self):
        if self.parity != "BTC":
            desired_db = [
                f"{self.parity}USDT-15m",
                f"{self.parity}USDT-1h",
                f"{self.parity}USDT-4h",
                f"{self.parity}USDT-1d",
                f"{self.parity}BTC-15m",
                f"{self.parity}BTC-1h",
                f"{self.parity}BTC-4h",
                f"{self.parity}BTC-1d",
            ]
        else:
            desired_db = [
                f"{self.parity}USDT-15m",
                f"{self.parity}USDT-1h",
                f"{self.parity}USDT-4h",
                f"{self.parity}USDT-1d",
            ]

        desiredlist = {
            desired_db[i]: self.api_data[desired_db[i]] for i in range(len(desired_db))
        }
        desired_dict = {}

        for db_name, db in desiredlist.items():
            db.reset_index(drop=True, inplace=True)
            crossing_points2 = db[
                (db["close"] > db["close_21_ema"].shift(1))
                & (db["close"].shift(1) <= db["close_21_ema"])
            ]
            c_points_down = self.crossing_points_down(db)
            ema_conditions = self.find_condition_unique(crossing_points2)
            crossing_points = crossing_points2.index.tolist()
            rise_percent = self.excel_data.excel_run_percent_up(db_name)
            df_dict = {}

            for i in crossing_points:
                if i >= 51:
                    prev_dataset = db.iloc[i - 51 : i]
                    atl = prev_dataset.loc[
                        prev_dataset["low"] == prev_dataset["low"].min(), "low"
                    ].values[0]
                    timee = db.at[i, "time"]
                    condition_stat = ema_conditions.at[timee, "condition"]

                    for condition, r_values in rise_percent.items():
                        if condition_stat == condition:
                            resistance_list = [atl + (atl * float(i)) for i in r_values]
                            df_dict[f"{timee}"] = resistance_list

            df = pd.DataFrame(df_dict)
            date_condition = (
                c_points_down.iloc[-1]["time"] < crossing_points2.iloc[-1]["time"]
            )
            current_price = db.iloc[-1]["close"]

            if not df.empty and date_condition:
                last_data = df[df.columns[-1]].tolist()
                adjusted_data = [i for i in last_data if not math.isnan(i)]
                desired_dict[db_name] = [i for i in adjusted_data if current_price > i]
            else:
                desired_dict[db_name] = (
                    "resistance levels are not valid since no cross up is observed.."
                )
        logging.info("Support levels were calculated..")
        return desired_dict

    def find_key_levels(self):
        try:
            desiredlist = {
                db_name: db
                for db_name, db in self.api_data.items()
                if db_name.endswith("USDT-1d") or db_name.endswith("USDT-1w")
            }
            main_dict = {}

            for db_name, db in desiredlist.items():
                low_data = db.iloc[-2]["low"]
                high_data = db.iloc[-2]["High"]
                mean_data = (high_data + low_data) / 2
                main_dict[db_name] = [low_data, mean_data, high_data]

            return main_dict

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            logging.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def calculate_key_levels(self, binance_data):
        try:
            current_price = binance_data[f"{self.parity}USDT-15m"].iloc[-1]["close"]
            key_levels = self.find_key_levels()
            data_dict = []

            for key, value in key_levels.items():
                if current_price < value[0] and key.endswith("1d"):
                    percentage = ((value[0] - current_price) / current_price) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage,3)} below the previous day low price"
                    )

                elif value[1] > current_price > value[0] and key.endswith("1d"):
                    percentage_1 = ((value[1] - current_price) / current_price) * 100
                    percentage_2 = ((current_price - value[0]) / value[0]) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage_1,3)} below the previous day mid price"
                    )
                    data_dict.append(
                        f"Current price is %{round(percentage_2,3)} above the previous day low price"
                    )

                elif value[2] > current_price > value[1] and key.endswith("1d"):
                    percentage_3 = ((value[2] - current_price) / current_price) * 100
                    percentage_4 = ((current_price - value[1]) / value[1]) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage_3,3)} below the previous day high price"
                    )
                    data_dict.append(
                        f"Current price is %{round(percentage_4,3)} above the previous day mid price"
                    )

                elif current_price > value[2] and key.endswith("1d"):
                    percentage_5 = ((current_price - value[2]) / value[2]) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage_5,3)} above the previous day high price"
                    )

                elif current_price < value[0] and key.endswith("1w"):
                    percentage = ((value[0] - current_price) / current_price) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage,3)} below the previous week low price"
                    )

                elif value[1] > current_price > value[0] and key.endswith("1w"):
                    percentage_1 = ((value[1] - current_price) / current_price) * 100
                    percentage_2 = ((current_price - value[0]) / value[0]) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage_1,3)} below the previous week mid price"
                    )
                    data_dict.append(
                        f"Current price is %{round(percentage_2,3)} above the previous week low price"
                    )

                elif value[2] > current_price > value[1] and key.endswith("1w"):
                    percentage_3 = ((value[2] - current_price) / current_price) * 100
                    percentage_4 = ((current_price - value[1]) / value[1]) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage_3,3)} below the previous week high price"
                    )
                    data_dict.append(
                        f"Current price is %{round(percentage_4,3)} above the previous week mid price"
                    )

                elif current_price > value[2] and key.endswith("1w"):
                    percentage_5 = ((current_price - value[2]) / value[2]) * 100
                    data_dict.append(
                        f"Current price is %{round(percentage_5,3)} above the previous week high price"
                    )

            logging.info("Key levels were calculated..")
            return data_dict

        except KeyError as e:
            error_message = f"KeyError occurred: {e}"
            logging.error(error_message, exc_info=True)
            raise ValueError(error_message)
