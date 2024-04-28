import pandas as pd

import logging
from logs import SingletonLogger


class MarketShiftAnalyzer:
    def __init__(self, api_data, parity) -> None:
        self.logger = SingletonLogger.get_logger()
        self.api_data = api_data
        self.logger.info("Market shift Instance initialized..")

    def find_market_shift_close(self):
        try:
            db_cond_satisfied = {}
            for db_name, db in self.api_data.items():
                market_shift_list = []
                for i in range(len(db)):
                    price = db.iloc[i]["close"]

                    if i >= 40:
                        min_price = db.iloc[i - 40 : i]["low"].min()
                        max_price = db.iloc[i - 40 : i]["High"].max()
                    else:
                        min_price = db.iloc[:i]["low"].min()
                        max_price = db.iloc[:i]["High"].max()

                    # Check if the current price is a new ATL or ATH
                    if price <= min_price:
                        market_shift_list.append((db.iloc[i]["time"], "down.."))
                    elif price >= max_price:
                        market_shift_list.append((db.iloc[i]["time"], "up.."))

                db_cond_satisfied[db_name] = market_shift_list

            self.logger.info("Market shift close analysis completed successfully.")
            return db_cond_satisfied

        except Exception as e:
            error_message = (
                f"An unexpected error occurred during market shift analysis: {e}"
            )
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)

    def find_market_shift_sensitive(self):
        try:
            db_cond_satisfied = {}
            for db_name, db in self.api_data.items():
                market_shift_list = []
                for i in range(len(db)):
                    price_high = db.iloc[i]["High"]
                    price_low = db.iloc[i]["low"]

                    if i >= 40:
                        min_price = db.iloc[i - 40 : i]["low"].min()
                        max_price = db.iloc[i - 40 : i]["High"].max()
                    else:
                        min_price = db.iloc[:i]["low"].min()
                        max_price = db.iloc[:i]["High"].max()

                    # Check if the current price is a new ATL or ATH
                    if price_low <= min_price:
                        market_shift_list.append((db.iloc[i]["time"], "down.."))
                    elif price_high >= max_price:
                        market_shift_list.append((db.iloc[i]["time"], "up.."))

                if len(market_shift_list) == 0:
                    db_cond_satisfied[db_name] = "No market shift observed.."
                else:
                    db_cond_satisfied[db_name] = market_shift_list

                self.logger.info(
                    "Sensitive Market shift analysis successfully completed for timeframe: "
                    + db_name
                )
            return db_cond_satisfied

        except Exception as e:
            error_message = (
                f"An unexpected error occurred during market shift analysis: {e}"
            )
            self.logger.error(error_message, exc_info=True)
            raise ValueError(error_message)
