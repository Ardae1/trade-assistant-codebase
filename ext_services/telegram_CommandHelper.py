from telegram.ext import Updater, CommandHandler
from telegram import Update
import multiprocess_trial as mp
from configs.config import Config

config = Config()


def analyze_coin(update: Update, context):
    if len(context.args) == 0:
        update.message.reply_text("Please provide a coin name.")
    coin_name = context.args[0].upper()
    if coin_name.endswith("USDT") or coin_name.endswith("BTC"):
        update.message.reply_text(
            f"Please remove USDT or BTC at the end and try again.."
        )
        return
    else:
        update.message.reply_text(
            f"analyzing for coins: {coin_name}USDT and {coin_name}BTC ..."
        )
    try:
        mp.main_engine(coin_name, config)
        update.message.reply_text(f"analysis completed.. Check the link above.")
    except Exception as e:
        update.message.reply_text(str(e))


def add_coin(update: Update, context):
    coin_list = config.get_coin_list()

    if len(context.args) == 0:
        update.message.reply_text("Please provide a coin name.")
        return

    coin_name = context.args[0].upper()
    coin_pair = [coin_name + "USDT", coin_name + "BTC"]

    if not any(item in coin_list.values for item in coin_pair):
        with open(config.COINLIST_PATH, "a") as file:
            for name in coin_pair:
                file.write(name + "\n")
        update.message.reply_text(f"{coin_name} successfully added to the list!")
    else:
        update.message.reply_text(f"{coin_name} parities are already in the list.")


def main() -> None:
    updater = Updater(config.TELEGRAM_TOKEN_PATH, use_context=True)

    db = updater.dispatcher
    task_add_coin = CommandHandler("add_coin", add_coin)
    task_analyze_coin = CommandHandler("analyze_coin", analyze_coin)

    db.add_handler(task_add_coin)
    db.add_handler(task_analyze_coin)

    updater.start_polling()

    updater.idle()


if __name__ == "__main__":
    main()
