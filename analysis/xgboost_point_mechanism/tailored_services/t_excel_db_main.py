import pandas as pd
import ast


def excel_run_rsi(file,rsi, parity):
        parite_usdt = parity + "USDT"
        parite_btc = parity + "BTC"
        xl1 = file.parse(f"RSI_{rsi}")
        xl1.set_index("parity", inplace=True)
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: list(ast.literal_eval(x)))
        
        rsii = {}
        if parity == "BTC":
            rsii["usdt_low"] = xl1.loc[parite_usdt]["RSI_Low_Range"]
            rsii["usdt_high"] = xl1.loc[parite_usdt]["RSI_High_Range"]
        else:
            rsii["usdt_low"] = xl1.loc[parite_usdt]["RSI_Low_Range"]
            rsii["btc_low"] = xl1.loc[parite_btc]["RSI_Low_Range"]
            rsii["usdt_high"] = xl1.loc[parite_usdt]["RSI_High_Range"]
            rsii["btc_high"] = xl1.loc[parite_btc]["RSI_High_Range"]

        return rsii


def excel_run_percent_down(db_name,file):
        if db_name.endswith("15m"):
            timeline = db_name[-3::]
        else:
            timeline = db_name[-2::]
        xl1 = file.parse(f"Fall_percentage_{timeline}")
        for col in xl1.columns:
            xl1[col] = xl1[col].map(lambda x: None if x == " " else x)
        return xl1