import pandas as pd
import numpy as np
from tailored_services import t_excel_db_main as texcel
import math


def find_condition_unique(db):
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

            print(dbasee["time"], "decent_condition(up)..")
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

            print(dbasee["time"], "decent_condition(down)..")
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

            print(dbasee["time"], "third_condition(up)..")
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

            print(dbasee["time"], "third_condition(down)..")
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

            print(dbasee["time"], "before_third_condition(up)..")
            condition_list.append([dbasee["time"], "before_third_condition(up)"])
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
            print(dbasee["time"], "before_third_condition(down)..")
            condition_list.append([dbasee["time"], "before_third_condition(down)"])
            continue

        elif (
            (dbasee["close_5_ema"] > dbasee["close_21_ema"])
            and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
            and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
            and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
            and (dbasee["close_21_ema"] > dbasee["close_100_ema"])
        ):

            print(dbasee["time"], "second_condition(up)..")
            condition_list.append([dbasee["time"], "second_condition(up)"])
            continue

        elif (
            (dbasee["close_5_ema"] < dbasee["close_21_ema"])
            and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
            and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
            and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
            and (dbasee["close_21_ema"] < dbasee["close_100_ema"])
        ):

            print(dbasee["time"], "second_condition(down)..")
            condition_list.append([dbasee["time"], "second_condition(down)"])
            continue

        elif (
            (dbasee["close_5_ema"] > dbasee["close_21_ema"])
            and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
            and (dbasee["close_5_ema"] > dbasee["close_100_ema"])
            and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
        ):

            print(dbasee["time"], "before_second_condition(up)..")
            condition_list.append([dbasee["time"], "before_second_condition(up)"])
            continue

        elif (
            (dbasee["close_5_ema"] < dbasee["close_21_ema"])
            and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
            and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
            and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
        ):

            print(dbasee["time"], "before_second_condition(down)..")
            condition_list.append([dbasee["time"], "before_second_condition(down)"])
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

            print(dbasee["time"], "first_condition(up)..")
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

            print(dbasee["time"], "first_condition(down)..")
            condition_list.append([dbasee["time"], "first_condition(down)"])
            continue

        elif ((dbasee["close_5_ema"] > dbasee["close_21_ema"])) or (
            (dbasee["close_5_ema"] > dbasee["close_21_ema"])
            and (dbasee["close_5_ema"] > dbasee["close_60_ema"])
        ):

            print(dbasee["time"], "before_first_condition(up)..")
            condition_list.append([dbasee["time"], "before_first_condition(up)"])
            continue

        elif ((dbasee["close_5_ema"] < dbasee["close_21_ema"])) or (
            (dbasee["close_5_ema"] < dbasee["close_21_ema"])
            and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
        ):

            print(dbasee["time"], "before_first_condition(down)..")
            condition_list.append([dbasee["time"], "before_first_condition(down)"])
            continue

        else:
            print(dbasee["time"], "Unknown condition..")
            condition_list.append([dbasee["time"], "Unknown condition"])
            continue

    df = pd.DataFrame(
        {
            "time": [data[0] for data in condition_list],
            "condition": [data[1] for data in condition_list],
        }
    )

    df.set_index("time", inplace=True)

    return df


def find_condition(data):
    db_conditions = {}
    for db_name, db in data.items():
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

                condition_list.append([dbasee["time"], "before_third_condition(up)"])
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

                condition_list.append([dbasee["time"], "before_third_condition(down)"])
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

                condition_list.append([dbasee["time"], "before_second_condition(up)"])
                continue

            elif (
                (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                and (dbasee["close_5_ema"] < dbasee["close_100_ema"])
                and (dbasee["close_21_ema"] < dbasee["close_60_ema"])
            ):

                condition_list.append([dbasee["time"], "before_second_condition(down)"])
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

                condition_list.append([dbasee["time"], "before_first_condition(up)"])
                continue

            elif ((dbasee["close_5_ema"] < dbasee["close_21_ema"])) or (
                (dbasee["close_5_ema"] < dbasee["close_21_ema"])
                and (dbasee["close_5_ema"] < dbasee["close_60_ema"])
                and (dbasee["close_21_ema"] > dbasee["close_60_ema"])
                and (dbasee["close_100_ema"] > dbasee["close_200_ema"])
                and (dbasee["close_60_ema"] > dbasee["close_100_ema"])
            ):

                condition_list.append([dbasee["time"], "before_first_condition(down)"])
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


def find_market_shift_sensitive(data):
    try:
        db_cond_satisfied = {}
        for db_name, db in data.items():
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
                    market_shift_list.append(
                        {"time": db.iloc[i]["time"], "shift": "down"}
                    )
                elif price_high >= max_price:
                    market_shift_list.append(
                        {"time": db.iloc[i]["time"], "shift": "up"}
                    )
            if not market_shift_list:
                db_cond_satisfied[db_name] = "No market shift observed."
            else:
                db_cond_satisfied[db_name] = pd.DataFrame(market_shift_list)

        return db_cond_satisfied

    except Exception as e:
        error_message = (
            f"An unexpected error occurred during market shift analysis: {e}"
        )

        raise ValueError(error_message)


def crossing_points_up(data):
    # Find the crossing points where the price crosses above the EMA 21
    crossing_points = data[
        (data["close"] > data["close_21_ema"].shift(1))
        & (data["close"].shift(1) <= data["close_21_ema"])
    ]
    return crossing_points


def crossing_points_down(data):
    # Find the crossing points where the price crosses below the EMA 21
    crossing_points = data[
        (data["close"] < data["close_21_ema"].shift(1))
        & (data["close"].shift(1) >= data["close_21_ema"])
    ]
    return crossing_points


def count_candles_s(excelfile, coin, main_data, rsi_freq):
    try:
        ema21_down_dict = {}
        for db_name, db in main_data.items():
            ema21_down = []
            rsi_levels = texcel.excel_run_rsi(excelfile, 14, coin)
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

            for i in crossing_points:
                remaining_rows = len(db) - i
                unique_dataset = (
                    db.iloc[i + 1 : i + 41]
                    if remaining_rows >= 40
                    else db.iloc[i + 1 :]
                )

                unique_time_row = db.iloc[i]

                if unique_time_row["time"] in rsi_sma_50["time"].values:
                    for k in range(1, len(unique_dataset)):
                        rsi_value = unique_dataset.iloc[k][f"rsi_{rsi_freq}"]
                        rsi_condition = (
                            unique_dataset.iloc[k][f"rsi_{rsi_freq}_sma"] >= 50
                        )
                        prev_candles = (
                            unique_dataset.iloc[:k]["close"]
                            < unique_dataset.iloc[:k]["close_21_ema"]
                        )

                        if (
                            unique_dataset.iloc[k]["close"]
                            < unique_dataset.iloc[k]["close_21_ema"]
                            and rsi_condition
                            and prev_candles.all()
                        ):

                            if 39.7 < rsi_value < 50 and 1 < k <= 8:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "ema above 50",
                                        "k": k,
                                        "range": "range2",
                                    }
                                )
                            elif rsi_low[0] < rsi_value < rsi_low[1] and 1 < k <= 8:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "ema above 50",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )
                            elif 23 < rsi_value < 30 and 1 < k <= 8:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "ema above 50",
                                        "k": k,
                                        "range": "range3",
                                    }
                                )
                            elif (
                                rsi_low[0] < rsi_value < rsi_low[1]
                                and second_tag < k <= fourth_tag
                            ):
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "ema below 50",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )
                            elif (
                                rsi_low[0] < rsi_value < rsi_low[1]
                                and 1 < k <= second_tag
                            ):
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "ema below 50",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )
                            elif 23 < rsi_value < 30 and 1 < k <= 8:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "ema below 50",
                                        "k": k,
                                        "range": "range3",
                                    }
                                )

                else:
                    for k in range(1, len(unique_dataset)):
                        rsi_value = unique_dataset.iloc[k][f"rsi_{rsi_freq}"]
                        prev_candles = (
                            unique_dataset.iloc[:k]["close"]
                            < unique_dataset.iloc[:k]["close_21_ema"]
                        )

                        if (
                            unique_dataset.iloc[k]["close"]
                            < unique_dataset.iloc[k]["close_21_ema"]
                            and prev_candles.all()
                        ):

                            if rsi_low[0] < rsi_value < rsi_low[1] and k > 25:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level4",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )
                            elif (
                                rsi_low[0] < rsi_value < rsi_low[1]
                                and second_tag < k <= fourth_tag
                            ):
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level3",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )
                            elif 39.7 < rsi_value < 50 and first_tag < k <= second_tag:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level2",
                                        "k": k,
                                        "range": "range2",
                                    }
                                )
                            elif 23 < rsi_value < 30 and first_tag < k <= second_tag:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level2",
                                        "k": k,
                                        "range": "range3",
                                    }
                                )
                            elif (
                                rsi_low[0] < rsi_value < rsi_low[1]
                                and first_tag < k <= second_tag
                            ):
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level2",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )
                            elif 23 < rsi_value < 30 and 1 < k <= first_tag:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level1",
                                        "k": k,
                                        "range": "range3",
                                    }
                                )
                            elif 39.7 < rsi_value < 50 and 1 < k <= first_tag:
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level1",
                                        "k": k,
                                        "range": "range2",
                                    }
                                )
                            elif (
                                rsi_low[0] < rsi_value < rsi_low[1]
                                and 1 < k <= first_tag
                            ):
                                ema21_down.append(
                                    {
                                        "time": unique_dataset.iloc[k]["time"],
                                        "level": "Level1",
                                        "k": k,
                                        "range": "range1",
                                    }
                                )

            ema21_down_dict[db_name] = pd.DataFrame(ema21_down)

        return ema21_down_dict

    except KeyError as e:
        error_message = f"KeyError occurred: {e}"
        raise ValueError(error_message)


def support_finder_test(dblist, excelfile, parity):

    if parity != "BTC":
        desired_db = [f"{parity}USDT-1h", f"{parity}BTC-1h"]
    else:
        desired_db = [f"{parity}USDT-1h"]

    desiredlist = {desired_db[i]: dblist[desired_db[i]] for i in range(len(desired_db))}
    support_dict = {}
    for db_name, db in desiredlist.items():
        total_support_list = []
        db.reset_index(drop=True, inplace=True)
        crossing_points2 = db[
            (db["close"] < db["close_21_ema"].shift(1))
            & (db["close"].shift(1) >= db["close_21_ema"])
        ]
        c_points_up = crossing_points_up(db)
        ema_conditions = find_condition_unique(crossing_points2)
        crossing_points = crossing_points2.index.tolist()
        fall_percent = texcel.excel_run_percent_down(db_name, excelfile)
        df_dict = {}
        df_list = []
        counter_index = 0
        c_point_length = len(c_points_up)
        cross_point_length = len(crossing_points2)

        for i in crossing_points:
            if i >= 51:
                if (
                    counter_index < c_point_length
                    and counter_index < cross_point_length
                ):
                    date_condition = (
                        crossing_points2.iloc[counter_index]["time"]
                        < c_points_up.iloc[counter_index]["time"]
                    )

                    print(f" {db_name} i :{counter_index}")
                    counter_index += 1

                    if date_condition:
                        prev_dataset = db.iloc[i - 51 : i]
                        ath = prev_dataset.loc[
                            prev_dataset["High"] == prev_dataset["High"].max(), "High"
                        ].values[0]
                        timee = db.at[i, "time"]
                        condition_stat = ema_conditions.at[timee, "condition"]

                        for condition, c_values in fall_percent.items():
                            if condition_stat == condition:
                                inner_list = []
                                for j in range(len(c_values)):
                                    support_level = ath - (ath * float(c_values[j]))
                                    inner_list.append(
                                        {
                                            "condition": condition,
                                            "fall_percent": c_values[j],
                                            "support_level": support_level,
                                        }
                                    )
                                df_dict[timee] = pd.DataFrame(inner_list)

        support_dict[db_name] = df_dict

    return support_dict


def calculate_optimum_trade_entry(db_dict): #can be improved!! not exactly doing what I want!   #DONE
    
    dbd={}
    surge_dict={'15m':0.06,'1h':0.10,'4h':0.20}
    range_dict={}
    for db_name, db in db_dict.items():
        db.reset_index(inplace=True)
        surge_percent=surge_dict[db_name[-3:] if db_name.endswith('15m') else db_name[-2:]]
        dict_list=[]
        ranges = {'optimum buy range': [], 'discount range': [], 'high_ote': [], 'low_ote': [],'high_disc': [], 'low_disc': []}
        for indx in range(0,len(db.index +1),30):
            try:
                if indx >= 75:
                    #These points should remain same while scanning the price for ote and disc!
                    print(f'indx: {indx}')
                    recent_high = db["high"][indx - 31 : indx].max()
                    recent_low = db["low"][indx - 74: indx - 32].min()

                    print(recent_high, recent_low)

                    # Calculate the midpoint coefficients and the midpoints
                    midpoint_coef1 = (recent_high - recent_low) * 0.786
                    midpoint_coef2 = (recent_high - recent_low) * 0.618
                    midpoint_coef3 = (recent_high - recent_low) * 0.5
                    midpoint = recent_high - midpoint_coef3
                    midpoint1 = recent_high - midpoint_coef1
                    midpoint2 = recent_high - midpoint_coef2

                    # Get the current price from the DataFrame (assuming it is the last close price)
                    for i in range(indx,indx +35):
                        print(f'i : {i}')
                        current_price = db.iloc[i]['close']
                        next_period_max=db.iloc[i+1:i+31]["high"].max()
                    
                        condition1= (next_period_max - current_price) / current_price >= surge_percent

                        if condition1:
                            if current_price > midpoint1 and current_price < midpoint2:
                                dict_list.append("optimum buy range")
                                ranges['optimum buy range'].append(indx)
                                ranges['high_ote'].append(db[db['high'] == db["high"][indx - 31 : indx].max()].index)
                                ranges['low_ote'].append(db[db['low'] == db["low"][indx - 74: indx - 32].min()].index)
                            elif current_price < midpoint and current_price > midpoint2:
                                dict_list.append("discount range")
                                ranges['discount range'].append(indx)
                                ranges['high_disc'].append(db[db['high'] == db["high"][indx - 31 : indx].max()].index)
                                ranges['low_disc'].append(db[db['low'] == db["low"][indx - 74: indx - 32].min()].index)
                            
                            else:
                                continue
                else:
                    continue
            except:
                print('errorrr...')
            
        dbd[db_name] = dict_list
        range_dict[db_name] = ranges
    
    return dbd, range_dict