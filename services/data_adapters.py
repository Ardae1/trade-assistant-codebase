import stockstats as st


class DataAdapters:
    # adds indicator numeric calculations as columns to OHLC API data.
    def indicator_data_load(self, dblist):
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

    # scales down the API dataframe to speed up calculations.
    def filtered_db_prep(self, dblist):
        for name in dblist.keys():
            # Check if the DataFrame has more than 400 rows
            if len(dblist[name]) > 400:
                # Remove the first 400 rows
                dblist[name] = dblist[name].iloc[400:]

        return dblist
