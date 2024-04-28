import datetime
import pandas as pd


class calculationHelper:
    def __init__(self, api_data, parity):
        self.api_data = api_data
        self.parity = parity

    def count_candle_s_pointer(self, combination):  # DONE
        levels = {
            "ema above 50": 45,
            "ema below 50": 50,
            "Level1": 10,
            "Level2": 30,
            "Level3": 20,
            "Level4": 50,
        }
        ranges = {"range1": 30, "range2": 24, "range3": 20}
        candle_numbers = list(range(1, 41))

        # define candle number ranges and their points
        candle_ranges = [(2, 6, 16), (7, 13, 14), (14, 17, 12), (25, 40, 20)]

        # function to find which range a candle number falls into and its points
        def find_range(num, ranges):
            for r in ranges:
                if r[0] <= num <= r[1]:
                    return r[2]
                else:
                    continue

        db_dictt = {}
        for db_name, db in combination.items():
            if isinstance(db, str):
                db_dictt[db_name] = 0
            else:
                combo = [db[1], db[3], int(db[2])]

                if (
                    "Level4" in combo[0]
                ):  # check if 'Level4' is in the first element of the combination
                    points = find_range(combo[2], candle_ranges)
                    if (
                        points is not None
                    ):  # check if points is not None before calculating total points
                        total_points = levels[combo[0]] + ranges[combo[1]] + points
                        db_dictt[db_name] = total_points
                elif combo[2] <= 17:
                    points2 = find_range(combo[2], [(2, 6, 4), (7, 13, 2), (14, 17, 3)])
                    if (
                        points2 is not None
                    ):  # check if points2 is not None before calculating total points
                        total_points = levels[combo[0]] + ranges[combo[1]] + points2
                        db_dictt[db_name] = total_points
                else:
                    db_dictt[db_name] = (
                        0  # return a tuple with 0 total points if none of the conditions are met
                    )

        return self.main_calculator(db_dictt) * 0.2

    def count_candles_r_pointer(self, combination):  # DONE
        levels = {
            "ema above 50-r": 45,
            "ema below 50-r": 50,
            "Level1-r": 10,
            "Level2-r": 30,
            "Level3-r": 20,
            "Level4-r": 50,
        }
        ranges = {"range1": 30, "range2": 24, "range3": 20}
        candle_numbers = list(range(1, 41))

        # define candle number ranges and their points
        candle_ranges = [(2, 6, 16), (7, 13, 14), (14, 17, 12), (25, 40, 20)]

        # function to find which range a candle number falls into and its points
        def find_range(num, ranges):
            for r in ranges:
                if r[0] <= num <= r[1]:
                    return r[2]
                else:
                    continue

        db_dictt = {}
        for db_name, db in combination.items():
            if isinstance(db, str):
                db_dictt[db_name] = 0
            else:
                combo = [db[1], db[3], int(db[2])]

                if (
                    "Level4-r" in combo[0]
                ):  # check if 'Level4' is in the first element of the combination
                    points = find_range(combo[2], candle_ranges)
                    if (
                        points is not None
                    ):  # check if points is not None before calculating total points
                        total_points = levels[combo[0]] + ranges[combo[1]] + points
                        db_dictt[db_name] = total_points
                elif combo[2] <= 17:
                    points2 = find_range(combo[2], [(2, 6, 4), (7, 13, 2), (14, 17, 3)])
                    if (
                        points2 is not None
                    ):  # check if points2 is not None before calculating total points
                        total_points = levels[combo[0]] + ranges[combo[1]] + points2
                        db_dictt[db_name] = total_points
                else:
                    db_dictt[db_name] = (
                        0  # return a tuple with 0 total points if none of the conditions are met
                    )

        return self.main_calculator(db_dictt) * 0.2

    def price_monitor_pointer(self, list):  # DONE
        listt = [i[0] for i in list]
        keywords = {
            "SKY": 30,
            "MELT": 30,
            "FLY": 30,
            "DUMP": 30,
            "BULL": 40,
            "BEAR LOW": 40,
            "HIGH": 50,
            "LOW": 50,
            "NONE": 0,
        }
        totalpoint = sum(keywords[j] for j in listt)

        return totalpoint * 0.2

    def support_finder_calculation(self, db_dict):
        range_percent = 0.03
        current_price = self.api_data[f"{self.parity}USDT-15m"].iloc[-1]["close"]
        db_dictt = {}
        for db_name, db in db_dict.items():
            if len(db) == 0:
                db_dictt[db_name] = 0

            for level in db:
                if isinstance(level, str):
                    db_dictt[db_name] = 0

                else:
                    percent_difference = abs(level - current_price) / current_price

                    if percent_difference <= range_percent:
                        db_dictt[db_name] = 100

                    else:
                        db_dictt[db_name] = 0

        return self.main_calculator(db_dictt) * 0.15

    def resistance_finder_calculation(self, db_dict):
        range_percent = 0.03
        current_price = self.api_data[f"{self.parity}USDT-15m"].iloc[-1]["close"]
        db_dictt = {}
        for db_name, db in db_dict.items():
            if len(db) == 0:
                db_dictt[db_name] = 0

            for level in db:
                if isinstance(level, str):
                    db_dictt[db_name] = 0

                else:
                    percent_difference = abs(level - current_price) / current_price

                    if percent_difference <= range_percent:
                        db_dictt[db_name] = 100

                    else:
                        db_dictt[db_name] = 0

        return self.main_calculator(db_dictt) * 0.15

    def calculate_optimum_trade_entry_buy(
        self, db_dict
    ):  # can be improved!! not exactly doing what I want!   #DONE
        db_dictt = {}
        dbd = {}
        for db_name, db in db_dict.items():
            recent_high = db["High"][-19:].max()
            recent_low = db["low"][-38:-19].min()

            # Calculate the midpoint coefficients and the midpoints
            midpoint_coef1 = (recent_high - recent_low) * 0.786
            midpoint_coef2 = (recent_high - recent_low) * 0.618
            midpoint_coef3 = (recent_high - recent_low) * 0.5
            midpoint = recent_high - midpoint_coef3
            midpoint1 = recent_high - midpoint_coef1
            midpoint2 = recent_high - midpoint_coef2

            # Get the current price from the DataFrame (assuming it is the last close price)
            current_price = db.iloc[-1]["close"]

            if current_price > midpoint1 and current_price < midpoint2:

                db_dictt[db_name] = 100
                dbd[db_name] = "current price is in optimum buy range"

            elif current_price < midpoint and current_price > midpoint2:

                db_dictt[db_name] = 60
                dbd[db_name] = "current price is in discount range"

            else:
                db_dictt[db_name] = 0
                dbd[db_name] = "Price is not in a range"

        return dbd, self.main_calculator(db_dictt) * 0.15

    def calculate_optimum_trade_entry_sell(
        self, db_dict
    ):  # can be improved!! not exactly doing what I want!   #DONE
        db_dictt = {}
        dbd = {}
        for db_name, db in db_dict.items():
            recent_low = db["low"][-19:].max()
            recent_high = db["High"][-38:-19].min()

            # Calculate the midpoint coefficients and the midpoints
            midpoint_coef1 = (recent_high - recent_low) * (1 - 0.786)
            midpoint_coef2 = (recent_high - recent_low) * (1 - 0.618)
            midpoint_coef3 = (recent_high - recent_low) * (0.5)
            midpoint = recent_high - midpoint_coef3
            midpoint1 = recent_high - midpoint_coef1
            midpoint2 = recent_high - midpoint_coef2

            # Get the current price from the DataFrame (assuming it is the last close price)
            current_price = db.iloc[-1]["close"]

            if current_price < midpoint1 and current_price > midpoint2:

                db_dictt[db_name] = 100
                dbd[db_name] = "current price is in optimum sell range"

            elif current_price > midpoint and current_price < midpoint2:

                db_dictt[db_name] = 60
                dbd[db_name] = "current price is in discount range"

            else:
                db_dictt[db_name] = 0
                dbd[db_name] = "Price is not in a range"

        return dbd, self.main_calculator(db_dictt) * 0.15

    def find_condition_pointer(self, db_dict):  # DONE
        db_dictt = {}
        points = {
            "decent_condition(up)": 100,
            "decent_condition(down)": 100,
            "third_condition(up)": 80,
            "third_condition(down)": 80,
            "before_third_condition(up)": 60,
            "before_third_condition(down)": 60,
            "second_condition(up)": 40,
            "second_condition(down)": 40,
            "before_second_condition(up)": 20,
            "before_second_condition(down)": 20,
            "first_condition(up)": 10,
            "first_condition(down)": 10,
            "before_first_condition(up)": 10,
            "before_first_condition(down)": 10,
        }
        for db_name, db in db_dict.items():
            last_status = db.iloc[-1]["condition"]
            time_condition_point = points[last_status]
            db_dictt[db_name] = time_condition_point

        return self.main_calculator(db_dictt) * 0.1

    def market_shift_pointer(self, db_dict):  # DONE
        db_dictt = {}
        for db_name, db in db_dict.items():

            if db_name.endswith("15m"):
                time_range = 15 * 5

            elif db_name.endswith("1h"):
                time_range = 15 * 20

            elif db_name.endswith("4h"):
                time_range = 15 * 80

            elif db_name.endswith("1d"):
                time_range = 15 * 480

            date_now = pd.Timestamp.now()
            date_range = [date_now, date_now - datetime.timedelta(minutes=time_range)]
            last_shift = db[-1]

            if isinstance(last_shift, str):
                db_dictt[db_name] = 0

            else:
                condition = (
                    last_shift[0] < date_range[0] and last_shift[0] > date_range[1]
                )
                if condition:
                    db_dictt[db_name] = 100

                else:
                    db_dictt[db_name] = 0

        return self.main_calculator(db_dictt) * 0.1

    def main_calculator(self, dict_list):
        timeframes = {
            "USDT-15m": 0.1,
            "USDT-1h": 0.15,
            "USDT-4h": 0.2,
            "USDT-1d": 0.25,
            "USDT-1w": 0.3,
            "BTC-15m": 0.1,
            "BTC-1h": 0.15,
            "BTC-4h": 0.2,
            "BTC-1d": 0.25,
            "BTC-1w": 0.3,
        }
        totalpoint = 0
        for db_name, db in dict_list.items():
            for timeframe, coef in timeframes.items():
                if db_name.endswith(timeframe):
                    totalpoint += db * coef
        return totalpoint

    def volume_pointer(self, result_message):
        message_points = {"actual": 60, "high": 100, "low": 0}
        result_point = 0
        for word, point in message_points.items():
            if result_message.find(word) != -1:
                result_point = message_points[word]
                break
            else:
                continue

        return result_point * 0.1
