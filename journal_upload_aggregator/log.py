import sys
import logging


LOG_FORMAT = (
    "%(asctime)s\t"
    "%(process)d\t"
    "%(filename)s:%(lineno)s\t"
    "%(name)s\t"
    "[%(levelname)s]\t"
    "%(message)s"
)


def initialize_logger(logger):
    # the root logger defaults to the WARNING log level.
    # this isn't acceptable as when starting up as debug, all debug messages
    # would be dropped until the root logger is configured. Setting to loglevel
    # to NOTSET causes all messages to be logged.
    logger.setLevel(logging.NOTSET)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def setup_logger(name):
    # loggers are organized as a tree, setup handlers on the root
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        initialize_logger(root_logger)
    return logging.getLogger(name)


LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


log_level_order = (
    logging.CRITICAL,
    logging.ERROR,
    logging.WARNING,
    logging.INFO,
    logging.DEBUG,
    logging.NOTSET,
)


def log_level_deduce(verbose_count, quiet_count, default_level=logging.INFO):
    i = log_level_order.index(default_level)
    i = min(len(log_level_order) - 1, max(0, i + verbose_count - quiet_count))
    return log_level_order[i]


def LogLevel(str_level):
    text_level = LOG_LEVELS.get(str_level, None)
    if text_level is not None:
        return text_level

    return int(str_level)
