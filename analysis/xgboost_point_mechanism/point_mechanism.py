import pandas as pd
from services.Binance_API_main import BinanceDataCollector
from configs.config import Config
from services.data_adapters import DataAdapters
from tailored_services import t_ema_conditional_calculations as tema
import time
import concurrent.futures


# All coins within each TF datasets will be grouped
# Each coin have 11000 row data (1h) - enough for model training (also BTC data can be included?)
# each grouped coin will be gone through the functions with a target result (for loop -> each coin goes through several functions and writes the result to the dataframe (1-0))
# A new dataframe will be created to hold coin - timestamp - function results (1-0)
# if the target price or percentage is reached within a specified time (after 20 candle etc.), it will add 1 to an function column, if not 0 will be added
# Repeat the same for each TF data
# How to define the target column?


# ------------- DATA PREP --------------------
def prep_data(parity):
    datalistusdt = [usdt_1h]
    datalistbtc = [btc_1h]
    df_name = [f"{parity}USDT-1h", f"{parity}BTC-1h"]
    data1 = [
        df[(df["parity"] == f"{parity}USDT")].reset_index(drop=True).set_index("parity")
        for df in datalistusdt
    ]
    data2 = [
        df[(df["parity"] == f"{parity}BTC")].reset_index(drop=True).set_index("parity")
        for df in datalistbtc
    ]
    data = data1 + data2
    df_dict = dict(zip(df_name, data))

    ind_load = adapter.indicator_data_load(df_dict)
    filtered_db = adapter.filtered_db_prep(ind_load)

    for dbname, db in filtered_db.items():
        db.reset_index(inplace=True)

    return filtered_db


# --------------- EMA CONDITION ---------------
def ema_condition_target(prep_data):
    expected_perc = {"15m": 0.06, "1h": 0.12, "4h": 0.25}
    # get target as a column
    for db_name, db in prep_data.items():
        db["emac"] = 0
        tf_perc = expected_perc[
            db_name[-3:] if db_name.endswith("15m") else db_name[-2:]
        ]
        print(tf_perc)
        for i in range(len(db) - 25):
            current_price = db.iloc[i]["close"]
            next_period_max = db.iloc[i + 1 : i + 26]["High"].max()
            next_period_min = db.iloc[i + 1 : i + 26]["low"].min()
            perc_condition = (
                next_period_max - current_price
            ) / current_price >= tf_perc
            print(f"perc_condition:{(next_period_max - current_price) / current_price}")

            db.loc[i, "emac"] = 1 if perc_condition else 0

    return prep_data


def ema_condition_test(prep_data):
    calc_dict = ema_condition_target(prep_data)
    ema_condition_result = tema.find_condition(prep_data)
    merged_dict = {
        key: pd.merge(calc_dict[key], ema_condition_result[key], how="outer")
        for key in calc_dict.keys()
        if key in ema_condition_result
    }
    return merged_dict


# --------------- Market Shift ---------------
def market_Shift_target(prep_data, main_data):
    shift_data = tema.find_market_shift_sensitive(prep_data)
    for db_name, db in main_data.items():
        db["shift"] = "No"

        for shift_index in range(len(shift_data[db_name])):
            print("mshift")
            shift_time = pd.to_datetime(shift_data[db_name].iloc[shift_index]["time"])
            shift_type = shift_data[db_name].iloc[shift_index]["shift"]

            # Update the corresponding rows in the original dataframe
            db.loc[pd.to_datetime(db["time"]) == shift_time, "shift"] = shift_type

    return main_data


# --------------- Count Candles - RSI ---------------
def count_candles_target(excelfile, coin, main_data, rsi_freq=14):
    anaylsis_result = tema.count_candles_s(excelfile, coin, t_Data, 14)
    for db_name, db in main_data.items():
        db["level"] = "No Level"
        db["candle_num"] = 0
        db["range"] = "No Range"

        for anaylsis_index in range(len(anaylsis_result[db_name])):
            analysis_time = pd.to_datetime(
                anaylsis_result[db_name].iloc[anaylsis_index]["time"]
            )
            print("eee")
            analysis_level = anaylsis_result[db_name].iloc[anaylsis_index]["level"]
            analysis_num = anaylsis_result[db_name].iloc[anaylsis_index]["k"]
            analysis_range = anaylsis_result[db_name].iloc[anaylsis_index]["range"]

            db.loc[pd.to_datetime(db["time"]) == analysis_time, "level"] = (
                analysis_level
            )
            db.loc[pd.to_datetime(db["time"]) == analysis_time, "candle_num"] = (
                analysis_num
            )
            db.loc[pd.to_datetime(db["time"]) == analysis_time, "range"] = (
                analysis_range
            )
    return main_data


# --------------- Support Finder ---------------
def support_level_target(c_data, excelfile, coin):
    support_levels = tema.support_finder_test(c_data, excelfile, coin)
    tolerance = 0.01

    for db_name, db in c_data.items():
        db["support_condition"] = "No Condition"
        db["fall_percent"] = 0.0
        db["support_level"] = 0.0

        support_data = support_levels[db_name]

        for index, row in db.iterrows():
            time = pd.to_datetime(row["time"])
            db_close_data = row["low"]

            support_data_keys = [pd.to_datetime(key) for key in support_data.keys()]

            if time in support_data_keys:
                print("TIME IN LIST..")
                support_info = support_data[time.strftime("%Y-%m-%d %H:%M:%S")]

                for support_index, support_row in support_info.iterrows():
                    input_up = support_row["support_level"] * (1 + tolerance)
                    input_down = support_row["support_level"] * (1 - tolerance)
                    print(input_up, input_down)
                    if input_down <= db_close_data <= input_up:
                        db.at[index, "support_level"] = support_row["support_level"]
                        db.at[index, "fall_percent"] = support_row["fall_percent"]
                        db.at[index, "support_condition"] = support_row["condition"]
            else:
                print("NO KEY DATA")

    return c_data


# --------------- Volume ---------------


# --------------- OTE ---------------
def calculate_optimum_trade_entry(
    db_dict,
):  # can be improved!! not exactly doing what I want!   #DONE
    main_dict = {}

    for db_name, db in db_dict.items():
        dbd = []
        for i in db.index:
            if i >= 20:
                recent_high = db["High"][i - 19 : i].max()
                recent_low = db["low"][i - 38 : i - 19].min()

                # Calculate the midpoint coefficients and the midpoints
                midpoint_coef1 = (recent_high - recent_low) * 0.786
                midpoint_coef2 = (recent_high - recent_low) * 0.618
                midpoint_coef3 = (recent_high - recent_low) * 0.5
                midpoint = recent_high - midpoint_coef3
                midpoint1 = recent_high - midpoint_coef1
                midpoint2 = recent_high - midpoint_coef2

                # Get the current price from the DataFrame (assuming it is the last close price)
                current_price = db.iloc[i]["close"]

                if current_price > midpoint1 and current_price < midpoint2:

                    dbd.append({"ote": "optimum buy range"})

                elif current_price < midpoint and current_price > midpoint2:

                    dbd.append({"ote": "discount range"})

                else:

                    dbd.append({"ote": "no range"})
        main_dict[db_name] = pd.DataFrame(dbd)

    return main_dict


def concat_ote(c_data):
    calc_dict = calculate_optimum_trade_entry(c_data)
    new_concat_dict = {}
    for db_name, db in c_data.items():
        concat_db = pd.concat([db, calc_dict[db_name]], axis=1)
        new_concat_dict[db_name] = concat_db

    return new_concat_dict


if __name__ == "__main__":
    start = time.time()

    # functions = [function1, function2, function3, function4, function5]

    config = Config()
    adapter = DataAdapters()

    usdt_1h = config.get_1h_usdt_excel_file()
    btc_1h = config.get_1h_btc_excel_file()
    excelfile = config.get_main_excel_file()
    coin = "FTM"
    p_data = prep_data(coin)

    data = ema_condition_test(p_data)

    t_Data = market_Shift_target(p_data, data)

    c_data = count_candles_target(excelfile, coin, t_Data, 14)

    s_data = support_level_target(c_data, excelfile, coin)
    o_data = concat_ote(s_data)
    #o_data["LINKUSDT-1h"].to_excel("dat.xlsx")

    end = time.time()

    print(end - start)
