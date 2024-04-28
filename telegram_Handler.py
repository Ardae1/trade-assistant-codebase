import requests
import pandas as pd
from github import Github
from fpdf import FPDF
from io import BytesIO
from urllib.parse import quote_plus
from config import Config
from excel_db_main import ExcelParserClass
from logs import SingletonLogger


class GithubPDFError(Exception):
    pass


class TelegramError(Exception):
    pass


class telegramHandler:
    def __init__(self, config: Config, excel_helper: ExcelParserClass) -> None:
        self.logger = SingletonLogger.get_logger()
        self.config = config
        self.excel_helper = excel_helper

    def github_pdf(self, results, pdf_name):
        token = self.config.GITHUB_TOKEN_PATH
        repo_owner = self.config.GITHUB_REPO_OWNER_PATH
        repo_name = self.config.GITHUB_REPO_NAME_PATH

        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=12)

        # Heading1
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "General Info", ln=True, align="L")
        pdf.ln(3)

        # General Info
        text_content = {
            "Coin Name": f"{results['coin_name']}",
            "Date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Current Price": f"{results['current_price']}",
            "Daily Bullish Average Target Price Range": results["daily_range_l"],
            "Daily Bearish Average Target Price Range": results["daily_range_h"],
            "Long Entry Range for Today": results["price_range_l"],
            "Short Entry Range for Today": results["price_range_h"],
            "Volume Info": self.excel_helper.volume_analyzer(
                results["coin_name"].split("USDT")[0]
            )[0],
            "Volume Status": self.excel_helper.volume_analyzer(
                results["coin_name"].split("USDT")[0]
            )[1],
            "Point": f"{round(results['total_sum'],2)}",
        }

        pdf.set_font("Arial", "B", 12)
        message = "\n".join(f"{key}: {value}" for key, value in text_content.items())
        pdf.multi_cell(0, 10, txt=message, border=0, align="L")
        pdf.ln(5)

        # Key levels anaylsis Heading
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Key Levels Analysis Results", ln=True, align="L")
        pdf.ln(3)

        # Key Levels Info

        pdf.set_font("Arial", "", 12)
        for i in results["key_levels"]:
            pdf.multi_cell(0, 10, txt=str(i), border=0, align="L")

        pdf.ln(3)

        # Count candles Heading
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "RSI - Candle Count Results by Timeframe", ln=True, align="L")
        pdf.ln(3)

        # Count Candles ınfo
        col_width = pdf.w / 2.5

        pdf.set_font("Arial", "", 10)

        dataset = [["Timeframe", "Results"]]
        for key, value in results["count_candles_s"].items():
            dataset.append([key, value[1:]])

        for row in dataset:
            for item in row:
                pdf.cell(col_width, 9, str(item), border=1)
            pdf.ln()

        pdf.ln(3)

        # market shift header
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Market Shift Results by Timeframe", ln=True, align="L")
        pdf.ln(3)

        # Market shift
        pdf.set_font("Arial", "", 10)

        last_data = None
        main_data = []

        for key, value in results["market_shift"].items():
            last_data = value[
                -1
            ]  # Get the last element of the list associated with the key
            main_data.append([key, last_data])

        for row in main_data:
            for item in row:
                pdf.cell(col_width, 9, str(item), border=1)
            pdf.ln()

        pdf.ln(3)

        # Price monitor heading
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Price Monitor Results", ln=True, align="L")
        pdf.ln(3)

        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Entry Levels", ln=True, align="L")
        pdf.ln(2)

        # price monitor
        pdf.set_font("Arial", "", 10)
        for k in results["daily_status"]:
            pdf.cell(0, 10, str(k), ln=True, align="L")

        pdf.ln(1)

        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Range Targets", ln=True, align="L")
        pdf.ln(2)

        pdf.set_font("Arial", "", 10)

        price_data = []
        for index, value in enumerate(results["price_monitor"]):
            price_data.append(value[1])

        for row in price_data:
            pdf.cell(0, 10, str(row), ln=True, align="L")

        pdf.ln(3)

        # find condition heading
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Find Condition Results by Timeframe", ln=True, align="L")
        pdf.ln(3)

        # find condition
        pdf.set_font("Arial", "", 10)

        trylb = []
        for db_name, db in results["condition"].items():
            trylb.append([db_name, db.iloc[-1]["condition"]])

        for row in trylb:
            for item in row:
                pdf.cell(col_width, 9, str(item), border=1)
            pdf.ln()

        pdf.ln(3)

        # optimum trade entry header
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Optimum Trade Entry Results by Timeframe", ln=True, align="L")
        pdf.ln(3)

        # Optimum trade entry
        pdf.set_font("Arial", "", 10)

        optdb = []
        for db_name, db in results["optimum_trade_entry_db"].items():
            optdb.append([db_name, db])

        for row in optdb:
            for item in row:
                pdf.cell(col_width, 9, str(item), border=1)
            pdf.ln()

        pdf.ln(3)

        # support calculation header
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Support Levels by Timeframe", ln=True, align="L")
        pdf.ln(3)

        # support levels
        sub_col_width_str = 100
        sub_col_width = 13
        pdf.set_font("Arial", "", 10)

        new_support_dict = {
            key: value
            for i, (key, value) in enumerate(results["support_calculation"].items())
            if i < 4
        }

        supdb = []
        for db_name, db in new_support_dict.items():
            if isinstance(db[0], float):
                round_db = [round(i, 3) for i in db]
                supdb.append([db_name, round_db[:8]])

            else:
                supdb.append([db_name, db])

        for row_title, sub_column_data in supdb:
            pdf.cell(col_width, 10, str(row_title), border=1)

            if isinstance(sub_column_data[0], float):
                for k in sub_column_data:
                    pdf.cell(sub_col_width, 10, str(k), border=1)

            else:
                pdf.cell(sub_col_width_str, 10, str(sub_column_data), border=1)

            pdf.ln()

        pdf_bytes = BytesIO(pdf.output(dest="S").encode("latin1"))

        try:
            g = Github(token)
            repo = g.get_repo(f"{repo_owner}/{repo_name}")

            pdf_file_name = f"{pdf_name}.pdf"
            content = pdf_bytes.getvalue()
            repo.create_file(pdf_file_name, "Upload PDF", content)

            pdf_url = (
                f"https://github.com/{repo_owner}/{repo_name}/blob/main/{pdf_file_name}"
            )
            self.logger.info(
                "PDF documentation of analysis was succesfully completed.."
            )
            return pdf_url
        except Exception as e:
            self.logger.error(
                f"An error occured while parsing PDF file: {e}", exc_info=True
            )
            raise GithubPDFError(f"An error occured while parsing PDF file: {e}")

    def telegram_message_prep(self, results):
        coin_name = results["coin_name"]
        count_candles_s = results["count_candles_s"][f"{coin_name}-15m"]
        condition = results["condition"][f"{coin_name}-15m"]

        body = {
            "Coin Name": f"*{coin_name}*",
            "Date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Current Price": f"*{results['current_price']}*",
            "Point": f"*{results['total_sum']}*",
            "Detailed Analysis Result": f"💡 Coin *{coin_name}* caught a high point for a long entry! Please go take a look! 💰",
            "15 min timeframe status": f"*{count_candles_s[1:4]}*",
            "Current Condition 15min timeframe ": f"*{condition[-1:]}*",
            "for detailed  analysis see PDF Report": self.github_pdf(
                results,
                f'{coin_name}-{pd.Timestamp.now().strftime("%Y-%m-%d/%H:%M:%S")}',
            ),
        }

        message = "\n".join(f"{key}: {value}" for key, value in body.items())
        url_safe_message = quote_plus(message)
        return url_safe_message

    def send_results_telegram(self, result_message):
        telegram_bot_token = self.config.TELEGRAM_TOKEN_PATH
        chat_ids = self.config.TELEGRAM_CHAT_ID_PATH
        try:
            for chat_id in chat_ids:
                bot_chat_id = chat_id
                result_message_escaped = result_message.replace("_", r"\_")
                send_text = (
                    "https://api.telegram.org/bot"
                    + telegram_bot_token
                    + "/sendMessage?chat_id="
                    + bot_chat_id
                    + "&parse_mode=Markdown&text="
                    + result_message_escaped
                )
                response = requests.get(send_text)
                self.logger.info(
                    "Analysis result has been sent to telegram bot channel.."
                )

                if response.status_code != 200:
                    raise ConnectionRefusedError(
                        "A HTTP error occured for Telegram API"
                    )

        except Exception as e:
            self.logger.error(
                f"An error occured while sending message to telegram bot: {e}",
                exc_info=True,
            )
            raise TelegramError(
                f"An error occured while sending message to telegram bot: {e}"
            )
