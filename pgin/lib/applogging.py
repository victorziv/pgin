import os
import sys
import logging
from lib import utils
logger = None
# ======================================


def set_handler_console(logger_name, out_to='stderr'):
    logformat = '%(asctime)s - %(levelname)-10s %(message)s'
    handler = logging.StreamHandler(getattr(sys, out_to))
    handler.set_name('%s-console' % logger_name)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(logformat))
    return handler
# ___________________________________________


def set_handler_file(logger_name, logpath):

    logformat = '%(asctime)s %(process)d %(levelname)-10s %(module)s %(funcName)-4s %(message)s'

    utils.create_directory(os.path.dirname(logpath))
    handler = logging.FileHandler(logpath)
    handler.set_name('%s-file' % logger_name)

    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(logformat))

    return handler
# ____________________________________________


def set_logger(logger_name, logpath, log_to_console=False, parent_logger=None):

    global logger
    logger = logging.getLogger(logger_name)

    # Debug logger
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    file_handler = set_handler_file(logger_name=logger_name, logpath=logpath)
    logger.addHandler(file_handler)
    logger.path = file_handler.baseFilename

    # Console logger
    if log_to_console:
        console_handler = set_handler_console(logger_name=logger_name)
        logger.addHandler(console_handler)

    if parent_logger is None:
        return logger

    for parent_lh in parent_logger.handlers:
        logger.addHandler(parent_lh)

    return logger
# ____________________________________________


def clean_logger(lg):
    for h in lg.handlers[:]:
        if hasattr(h, 'connection'):
            h.connection.close()
        h.close()
        lg.removeHandler(h)

    lg = None
# ====================================================
