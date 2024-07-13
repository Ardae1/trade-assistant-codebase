import sys
from configs.config import Config
from services.Binance_API_main import BinanceDataCollector
from services.data_adapters import DataAdapters
from services.excel_db_main import ExcelParserClass
from services.telegram_Handler import telegramHandler
import time
import pandas as pd
import requests
from urllib.parse import quote_plus
import logging


def coin_prep():
    coin_list = []
    prep_list = config.get_coin_list_wo_parity().values.tolist()
    for coin_w_bracket in prep_list:
        coin_list.append(coin_w_bracket[0])

    return coin_list


def get_rsi_ema_50_above_data(rsi_freq, filtered_data, excel_rsi_data):
    coin_dict = {}
    for db_name, db in filtered_data.items():
        coin_result = 0
        rsi_data = db[f"rsi_{rsi_freq}"]
        rsi_sma_50 = db[f"rsi_{rsi_freq}_sma"]

        if any(
            data > int(excel_rsi_data["usdt_low"][0])
            and data < int(excel_rsi_data["usdt_low"][1])
            for data in rsi_data.tail()
        ) and any(sma_data > 50 for sma_data in rsi_sma_50.tail()):
            coin_result += 1

        # if any(sma_data > 50 for sma_data in rsi_sma_50.tail()):
        #   coin_result += 1

        if coin_result == 1 and "USDT" in db_name:
            coin_dict[db_name] = 1
        elif "USDT" in db_name:
            coin_dict[db_name] = 0

    return coin_dict


def organize_tf_results_for_coins(main_result_list):
    categorized_tf_dict = {}
    for coin_name, values in main_result_list.items():
        for key, value in values.items():
            if value == 1:
                if key.endswith("USDT-15m"):
                    categorized_tf_dict["USDT-15m"] = coin_name
                if key.endswith("USDT-1h"):
                    categorized_tf_dict["USDT-1h"] = coin_name
                if key.endswith("USDT-4h"):
                    categorized_tf_dict["USDT-4h"] = coin_name
                if key.endswith("USDT-1d"):
                    categorized_tf_dict["USDT-1d"] = coin_name
                if key.endswith("USDT-1w"):
                    categorized_tf_dict["USDT-1w"] = coin_name
    return categorized_tf_dict


# alert mechanism that retrieve coins for a condition "RSI EMA above 50 + RSI below 50 and in low interval(35-44 range)" for TF (15m-1h-4h-1d)
def get_alert_for_rsi_condition(rsi_freq):
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG

    data_adapter = DataAdapters()
    list_of_coins = coin_prep()
    main_result_list = {}

    for coin in list_of_coins:
        try:
            logger.info(f"Processing coins: {list_of_coins}")

            binance_collector = BinanceDataCollector(coin, config)
            raw_api_data = binance_collector.unique_data_collector()
            loaded_indicator_data = data_adapter.indicator_data_load(raw_api_data)
            filtered_data = data_adapter.filtered_db_prep(loaded_indicator_data)

            excel_data = ExcelParserClass(coin, config, binance_collector)
            excel_rsi_data = excel_data.excel_run_rsi(rsi_freq)
            rsi_cond_result = get_rsi_ema_50_above_data(
                14, filtered_data, excel_rsi_data
            )

            logger.debug(f"RSI condition results completed for coin: {coin}")
            main_result_list[coin] = rsi_cond_result
            logger.debug(f"Results added to main list for coin: {coin}")

        except KeyError as e:
            error_message = f"KeyError occurred for coin: {coin} - {e}"
            logger.error(error_message)
            # raise ValueError(error_message)

    result_df = organize_tf_results_for_coins(main_result_list)
    return result_df


def send_to_telegram(result_df):
    telegram_bot_token = config.TELEGRAM_TOKEN_PATH
    chat_ids = config.TELEGRAM_CHAT_ID_PATH

    # Initialize message_body with empty lists
    message_body = {
        "1W": [],
        "1D": [],
        "4H": [],
        "1H": [],
        "15M": [],
    }

    # Iterate over keys in message_body
    for key in message_body.keys():
        # Check if the key exists as a column in result_df
        if key in result_df.keys():
            # Append values to message_body if the column exists
            message_body[key] = result_df[key].values.tolist()

    # Construct message
    message = "\n".join(f"{key}: {value}" for key, value in message_body.items())
    url_safe_message = quote_plus(message)
    result_message = url_safe_message.replace("_", r"\_")

    try:
        for chat_id in chat_ids:
            bot_chat_id = chat_id

            send_text = (
                "https://api.telegram.org/bot"
                + telegram_bot_token
                + "/sendMessage?chat_id="
                + bot_chat_id
                + "&parse_mode=Markdown&text="
                + result_message
            )
            response = requests.get(send_text)

            if response.status_code != 200:
                raise ConnectionRefusedError("A HTTP error occured for Telegram API")

    except Exception as e:
        raise ValueError(f"An error occured while sending message to telegram bot: {e}")


if __name__ == "__main__":
    start_time = time.time()
    config = Config()

    result_df = get_alert_for_rsi_condition(14)
    send_to_telegram(result_df)

    end_time = time.time()

    final = end_time - start_time
    print(final)
