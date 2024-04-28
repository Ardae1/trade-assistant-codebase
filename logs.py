import logging
import sys


class SingletonLogger:
    _instance = logging.getLogger(__name__)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)  # Set the level to DEBUG for all messages
    formatter = logging.Formatter(
        "%(asctime)s - line %(lineno)d - %(levelname)s - %(message)s"
    )
    stream_handler.setFormatter(formatter)
    _instance.addHandler(stream_handler)
    _instance.setLevel(logging.DEBUG)

    @classmethod
    def get_logger(cls):
        return cls._instance
