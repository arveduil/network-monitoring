import logging

from utils.time import get_now


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    launch_time = str(get_now()).replace(' ', '_')
    f_handler = logging.FileHandler(f'./logs/{logger_name}-{launch_time}.log')
    c_handler = logging.StreamHandler()

    c_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.DEBUG)
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)
    logger.addHandler(c_handler)
    return logger
