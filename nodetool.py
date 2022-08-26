"""Run nodetool commands on a Cassandra node."""
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os

import asyncssh
from node import Node

from remote import Remote

CURRENT_PATH = Path(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="Run a nodetool command on Cassandra node.")
parser.add_argument("command", help="The nodetool command")
parser.add_argument("-n", "--name", help="The node name", required=False)
parser.add_argument("-i", "--host", help="The node host", required=False)
parser.add_argument("-p", "--port", type=int, help="The node port", required=False)
parser.add_argument("-u", "--username", help="The node username", required=False)
parser.add_argument("-w", "--password", help="The node password", required=False)
parser.add_argument("-l", "--log-level", help="The logging level", choices=["INFO", "ERROR", "DEBUG"], default="INFO")
parser.add_argument("--log-file", default=f"{Path(__file__).stem}.log", help="The logging file")
parser.add_argument("--error-log-file", default=f"{Path(__file__).stem}.error.log", help="The error logging file")
args = parser.parse_args()

def get_remotes():
    with open("remotes.json") as fp:
        return json.load(fp)

def get_remote(name):
    return get_remotes().get(name, None)

def run_nodetool_command(command, name, host, port, username, password):
    remote = get_remote(name) or Remote(host, port, username, password, logger)
    node = Node(remote["host"], remote["port"], remote["user"], remote["password"], logger)
    node.run(f"nodetool {command}")


async def run_command(command, host, port, username, password, raise_error=False):
    async with asyncssh.connect(host=host, port=port, username=username, password=password) as connection:
        result = await connection.run(command)
        logger.debug(75*'-')
        logger.debug(host)
        logger.debug(len(host)*'*')
        logger.info(command)
        logger.info(f"Output:\n{result.stdout}")
        if result.stderr:
            logger.error(f"Error: {result.stderr}")
            if raise_error:
                raise Exception(f'Command Error: {command}::{result.stderr}')
        return result


def setup_logger(level, log_file, error_log_file):
    global logger
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


if __name__ == "__main__":
    setup_logger(args.log_level, args.log_file, args.error_log_file)
    run_nodetool_command(args.command, args.name, args.host, args.port, args.username, args.password)    
