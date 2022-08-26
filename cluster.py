
import argparse
import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import random

from node import NamedNode


def parse_args():
    """Parse the script arguments.

    :return: The parsed args
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Run command on a remote.")
    parser.add_argument("command", help="The command to run")
    parser.add_argument("-f", "--nodes-file", help="The nodes file", default="remotes.json", required=False)
    parser.add_argument("-l", "--log-level", help="The logging level", choices=["INFO", "ERROR", "DEBUG"], default="INFO")
    parser.add_argument("--log-file", default=f"{Path(__file__).stem}.log", help="The logging file")
    parser.add_argument("--error-log-file", default=f"{Path(__file__).stem}.error.log", help="The error logging file")
    return parser.parse_args()
    

class Cluster:

    def __init__(self, nodes_file, logger):
        self._nodes_file = nodes_file
        self._nodes = None
        self._logger = logger
        self._load_nodes()

    def get_nodes(self):
        return self._nodes
    
    def get_random_node(self):
        return random.choice(self._nodes)

    def _load_nodes(self):
        with open(self._nodes_file) as fp:
            self._nodes = [
                NamedNode(name, self._logger)
                for name in json.load(fp).keys()
            ]

    def restart(self):
        for node in self._nodes:
            node.restart()

    def is_up(self):
        is_up = all([node.is_active() for node in self._nodes])
        self._logger.info(f"All nodes are up: {is_up}")
        return all([node.is_active() for node in self._nodes])

    def status(self):
        node = self.get_random_node()
        return node.status()

    def info(self):
        tasks = (node.info(async_=True) for node in self._nodes)
        return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def setup_logger(level, log_file, error_log_file):
    """Set up a logger.

    :param str level: The log level
    :param str log_file: The log file
    :param str error_log_file: The error log file
    :return: The logger
    :rtype: logging.Logger
    """
    logger = logging.getLogger(__name__)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10000000,
        backupCount=0
    )
    error_file_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=10000000,
        backupCount=0
    )
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    file_handler.setFormatter(formatter)
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel("ERROR")
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(error_file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(level)
    return logger


if __name__=="__main__":
    args = parse_args()
    cluster = Cluster(
        args.nodes_file, 
        setup_logger(args.log_level, args.log_file, args.error_log_file)
    )
    if args.command == "restart":
        cluster.restart()
    elif args.command == "status":
        cluster.status()
    elif args.command == "info":
        cluster.info()
    else:
        raise Exception("Unknown command")
