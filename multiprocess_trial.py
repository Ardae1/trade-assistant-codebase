import concurrent.futures
import stockstats as st
from urllib.parse import quote_plus
import time
from functools import partial
from excel_db_main import ExcelParserClass
from ema_conditional_calculations import EMACalculationClass
from config import Config
from Binance_API_main import BinanceDataCollector
from telegram_Handler import telegramHandler
from point_calculation_helper import calculationHelper
from market_shift import MarketShiftAnalyzer


def indicator_data_load(dblist):
    dblist = {name: st.StockDataFrame(value) for name, value in dblist.items()}
    ema_list = [
        "close_5_ema",
        "close_21_ema",
        "close_60_ema",
        "close_100_ema",
        "close_200_ema",
    ]
    rsi_list = ["rsi_14", "rsi_21"]
    rsi_sma_list = ["rsi_14_sma", "rsi_21_sma"]
    bollinger_list = ["boll_20"]
    for name in dblist.keys():
        [dblist[name][str(i)] for i in ema_list]  # ema'ları ekler
        [dblist[name][str(i)] for i in rsi_list]  # rsi ekler
        [dblist[name][str(i)] for i in rsi_sma_list]  # rsi sma ekler
        [dblist[name][str(i)] for i in bollinger_list]  # bollinger ekler

    return dblist


def filtered_db_prep(dblist):
    for name in dblist.keys():
        # Check if the DataFrame has more than 400 rows
        if len(dblist[name]) > 400:
            # Remove the first 400 rows
            dblist[name] = dblist[name].iloc[400:]

    return dblist


def main_engine(parity, config_data: Config):
    binance_data = BinanceDataCollector(parity, config_data)
    unique_data = binance_data.unique_data_collector()
    loaded_indicator_data = indicator_data_load(unique_data)
    filtered_data = filtered_db_prep(loaded_indicator_data)
    mrkt = MarketShiftAnalyzer(filtered_data, parity)
    exceltry = ExcelParserClass(parity, config_data, binance_data)
    emaCalculation = EMACalculationClass(parity, filtered_data, exceltry)
    tlg_helper = telegramHandler(config_data, exceltry)
    calc_helper = calculationHelper(unique_data, parity)
    coin_name = parity + "USDT"
    current_price = filtered_data[f"{parity}USDT-15m"].iloc[-1]["close"]

    # Initialize the numeric, string and intermediate results dictionaries
    results_num = {}
    results_str = {"coin_name": coin_name, "current_price": current_price}
    intermediate_results = {}

    volume_anaylzer = exceltry.volume_analyzer(unique_data)[1]
    results_num["volume_point"] = calc_helper.volume_pointer(volume_anaylzer)
    intermediate_results["volume"] = volume_anaylzer

    count_candles_s = emaCalculation.count_candles_s(14)
    results_num["candle_count_r_pointer"] = calc_helper.count_candle_s_pointer(
        count_candles_s
    )
    intermediate_results["count_candles_s"] = count_candles_s

    market_shift = mrkt.find_market_shift_sensitive()
    results_num["market_shift_pointer"] = calc_helper.market_shift_pointer(market_shift)
    intermediate_results["market_shift"] = market_shift

    (
        price_monitor,
        daily_status,
        daily_range_h,
        daily_range_l,
        price_range_h,
        price_range_l,
    ) = exceltry.price_monitor(filtered_data)
    results_num["price_monitor_pointer"] = calc_helper.price_monitor_pointer(
        price_monitor
    )
    intermediate_results["price_monitor"] = price_monitor
    intermediate_results["daily_status"] = daily_status
    intermediate_results["daily_range_h"] = daily_range_h
    intermediate_results["daily_range_l"] = daily_range_l
    intermediate_results["price_range_h"] = price_range_h
    intermediate_results["price_range_l"] = price_range_l

    condition = emaCalculation.find_condition()
    results_num["condition_pointer"] = calc_helper.find_condition_pointer(condition)
    intermediate_results["condition"] = condition

    support_calculation = emaCalculation.support_finder(condition)
    results_num["support_calculation_result"] = calc_helper.support_finder_calculation(
        support_calculation
    )
    intermediate_results["support_calculation"] = support_calculation

    optimum_trade_entry0, optimum_trade_entry1 = (
        calc_helper.calculate_optimum_trade_entry_buy(filtered_data)
    )
    results_num["optimum_trade_entry"] = optimum_trade_entry1
    intermediate_results["optimum_trade_entry_db"] = optimum_trade_entry0

    intermediate_results["key_levels"] = emaCalculation.calculate_key_levels(
        unique_data
    )

    # Compute the total sum
    results_num["total_sum"] = sum(results_num.values())

    if results_num["total_sum"] > 0:
        # Merge the results dictionaries
        all_results = {**results_str, **results_num, **intermediate_results}
        message_temp = tlg_helper.telegram_message_prep(all_results)
        send_message = tlg_helper.send_results_telegram(message_temp)

    # Return the combined results
    return {**results_str, **results_num, **intermediate_results}


if __name__ == "__main__":
    config_data = Config()
    coin_list = config_data.get_coin_list().values

    coins = [
        "XLM",
        "RLC",
        "HIGH",
        "LINK",
        "ETH",
        "BTC",
        "WAVES",
        "REN",
        "COTI",
        "XTZ",
        "EGLD",
        "IOTA",
        "XRP",
        "LTC",
        "ADA",
        "OCEAN",
        "DOT",
        "KAVA",
        "RUNE",
        "FTM",
        "AVAX",
        "ATOM",
        "BLZ",
        "TOMO",
        "EOS",
        "AAVE",
        "QNT",
        "MATIC",
        "ENS",
        "STORJ",
        "ARPA",
        "LRC",
        "OGN",
        "THETA",
        "ALGO",
        "DYDX",
        "COMP",
        "MANA",
        "SAND",
        "FLM",
    ]

    num_cores = 7

    start_time = time.time()

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        partial_main_engine = partial(main_engine, config_data=config_data)
        executor.map(partial_main_engine, coins)

    end_time = time.time()

    final = end_time - start_time
    print(final)
