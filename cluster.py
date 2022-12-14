
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
    parser.add_argument("-k", "--keyspace", help="The keyspace to use", required=False)
    parser.add_argument("-t", "--table", help="The table to use", required=False)
    parser.add_argument("-e", "--cql-command", help="The CQL command to run", required=False)
    parser.add_argument("-l", "--log-level", help="The logging level", choices=["INFO", "ERROR", "DEBUG"], default="INFO")
    parser.add_argument("--log-file", default=f"logs/{Path(__file__).stem}.log", help="The logging file")
    parser.add_argument("--error-log-file", default=f"logs/{Path(__file__).stem}.error.log", help="The error logging file")
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
        is_up = all([node.is_up() for node in self._nodes])
        self._logger.info(f"All nodes are up: {is_up}")
        return all([node.is_up() for node in self._nodes])

    def status(self):
        node = self.get_random_node()
        return node.status()

    def info(self):
        return self._run(
            node.info(async_=True) 
            for node in self._nodes
        )

    def flush_table(self, keyspace, table):
        return self._run(
            node.flush_table(keyspace, table, async_=True)
            for node in self._nodes
        )

    def compactionstats(self):
        return self._run(
            node.compactionstats(async_=True) 
            for node in self._nodes
        )

    def find_table_compactions(self, keyspace, table):
        compactions = [
            item
            for node_compactions in self._run(
                node.find_table_compactions(keyspace, table, async_=True)
                for node in self._nodes
            )
            for item in node_compactions
        ]
        self._logger.info(f"Found compactions in cluster: {compactions}")
        return compactions

    def stop_table_compactions(self, keyspace, table):
        return [
            node.stop_table_compactions(keyspace, table, async_=True)
            for node in self._nodes
        ]

    def listsnapshots(self):
        return self._run(
            node.listsnapshots(async_=True) 
            for node in self._nodes
        )

    def find_table_snapshots(self, keyspace, table):
        snapshots = [
            item
            for node_snapshots in self._run(
                node.find_table_snapshots(keyspace, table, async_=True)
                for node in self._nodes
            )
            for item in node_snapshots
        ]
        self._logger.info(f"Found snapshots in cluster: {snapshots}")
        return snapshots

    def clear_table_snapshots(self, keyspace, table):
        return self._run(
            node.clear_table_snapshots(keyspace, table, async_=True) 
            for node in self._nodes
        )

    def repair_table(self, keyspace, table):
        return self._run(
            node.repair_table(keyspace, table, async_=False) 
            for node in self._nodes
        )
    
    def cleanup_table(self, keyspace, table):
        return self._run(
            node.cleanup_table(keyspace, table, async_=True) 
            for node in self._nodes
        )
    
    def compact_table(self, keyspace, table):
        return self._run(
            node.compact_table(keyspace, table, async_=True) 
            for node in self._nodes
        )

    def cqlsh(self, command):
        node = self.get_random_node()
        return node.cqlsh(command)

    def _run(self, tasks):
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
    elif args.command == "flush":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.flush_table(args.keyspace, args.table)
    elif args.command == "compactionstats":
        cluster.compactionstats()
    elif args.command == "find-table-compactions":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.find_table_compactions(args.keyspace, args.table)
    elif args.command == "stop-table-compactions":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.stop_table_compactions(args.keyspace, args.table)
    elif args.command == "listsnapshots":
        cluster.listsnapshots()
    elif args.command == "find-table-snapshots":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.find_table_snapshots(args.keyspace, args.table)
    elif args.command == "clear-table-snapshots":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.clear_table_snapshots(args.keyspace, args.table)
    elif args.command == "repair-table":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.repair_table(args.keyspace, args.table)
    elif args.command == "cleanup-table":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.cleanup_table(args.keyspace, args.table)
    elif args.command == "compact-table":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        cluster.compact_table(args.keyspace, args.table)
    elif args.command == "cqlsh":
        if not args.cql_command:
            raise argparse.error("CQL command should be specified!")
        cluster.cqlsh(args.cql_command)
    else:
        raise argparse.error("Unknown command")
