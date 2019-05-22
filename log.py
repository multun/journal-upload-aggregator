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
