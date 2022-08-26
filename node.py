import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
import time

from remote import Remote


def parse_args():
    """Parse the script arguments.

    :return: The parsed args
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Run command on a remote.")
    parser.add_argument("command", help="The command to run")
    parser.add_argument("-n", "--remote", help="The node remote", required=False)
    parser.add_argument("-i", "--host", help="The remote hostname", required=False)
    parser.add_argument("-p", "--port", type=int, help="The remote port", required=False)
    parser.add_argument("-u", "--username", help="The remote username", required=False)
    parser.add_argument("-w", "--password", help="The remote password", required=False)
    parser.add_argument("-l", "--log-level", help="The logging level", choices=["INFO", "ERROR", "DEBUG"], default="INFO")
    parser.add_argument("--log-file", default=f"{Path(__file__).stem}.log", help="The logging file")
    parser.add_argument("--error-log-file", default=f"{Path(__file__).stem}.error.log", help="The error logging file")
    args = parser.parse_args()
    if args.remote and any([args.host, args.port, args.username, args.password]):
        parser.error("Only one of the remote and custom remote can be specified.")
    if not args.remote and not all([args.host, args.port, args.username, args.password]):
        parser.error("All custom remote fields should be specified.")
    return args


class Node(Remote):
    
    def info(self, async_=False):
        return self._run("nodetool info", async_)

    def status(self, async_=False):
        return self._run("nodetool status", async_)

    def is_up(self, async_=False):
        result = bool(
            re.search(
            "[\w\s\S]*Gossip[\w\s\S]*true[\w\s\S]*Thrift[\w\s\S]*true[\w\s\S]*Transport[\w\s\S]*true[\w\s\S]*",
            self.info(async_)[0]
        ))
        self._logger.info(result)
        return result

    def restart(self, async_=False):
        self.stop(async_)
        self.start(async_)
        start_time = time.time()
        while time.time() - start_time < 300:
            if self.is_up(async_):
                return True
            time.sleep(2)
        raise TimeoutError("TimeOut occurred! Couldn't restart the node!")
    
    def start(self, async_=False):
        return self._run("sudo systemctl start cassandra", async_)

    def stop(self, async_=False):
        return self._run("sudo systemctl stop cassandra", async_)

    def _run(self, command, async_=False):
        return self.async_run(command) if async_ else self.run(command)


class NamedNode(Node):

    def __init__(self, name, logger):
        remote_data = self.get(name)
        if not remote_data:
            raise Exception("Remote doesn't exist!")
        super().__init__(remote_data["host"], remote_data["port"], remote_data["user"], remote_data["password"], logger)
    
    def read_remotes(self):
        """Read remotes.

        :return: The remotes
        :rtype: list[dict]
        """
        with open("remotes.json") as fp:
            return json.load(fp)


    def get(self, name):
        """Return remote by name.

        :param str name: The remote name
        :return: The remote data
        :rtype: dict
        """
        return self.read_remotes().get(name, None)


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
    node = NamedNode(
        args.remote, 
        setup_logger(args.log_level, args.log_file, args.error_log_file)
    ) if args.remote else Node(
        args.host,
        args.port,
        args.username,
        args.password,
        setup_logger(args.log_level, args.log_file, args.error_log_file)
    )

    if args.command == "restart":
        node.restart()
    if args.command == "start":
        node.start()
    if args.command == "stop":
        node.stop()
    elif args.command == "up":
        node.is_up()
    elif args.command == "status":
        node.status()
    elif args.command == "info":
        node.info()
    
    else:
        node.run(args.command)
