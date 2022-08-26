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
    parser.add_argument("-k", "--keyspace", help="The keyspace to use", required=False)
    parser.add_argument("-t", "--table", help="The table to use", required=False)
    parser.add_argument("-c", "--compaction-id", help="The compaction id", required=False)
    parser.add_argument("-e", "--cql-command", help="The CQL command to run", required=False)
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

    def flush_table(self, keyspace, table, async_=False):
        return self._run(f"nodetool flush -- {keyspace} {table}", async_)

    def compactionstats(self, async_=False):
        return self._run("nodetool compactionstats", async_)

    def find_table_compactions(self, keyspace, table, async_=False):
        if async_:
            return self.async_find_table_compactions(keyspace, table)
        output = self.compactionstats()[0]
        compactions = []
        for line in output.splitlines():
            compaction = self._parse_compaction_output(line)
            if compaction and compaction['keyspace'] == keyspace and compaction['table'] == table:
                compactions.append(compaction['id'])
        self._logger.info(f"Found compactions: {compactions}")
        return compactions
    
    async def async_find_table_compactions(self, keyspace, table):
        result = await self.compactionstats(async_=True)
        output = result[0]
        compactions = []
        for line in output.splitlines():
            compaction = self._parse_compaction_output(line)
            if compaction and compaction['keyspace'] == keyspace and compaction['table'] == table:
                compactions.append(compaction['id'])
        self._logger.info(f"Found compactions: {compactions}")
        return compactions
    
    def stop_table_compactions(self, keyspace, table, async_):
        compactions = self.find_table_compactions(keyspace, table)
        for compaction_id in compactions:
            self.stop_compaction(compaction_id, async_)

    def stop_compaction(self, compaction_id, async_=False):
        return self._run(f"nodetool stop -id {compaction_id}", async_)

    def _parse_compaction_output(self, text):
        matches = re.match('(?P<id>[0-9a-zA-Z-_]+)\s+(?P<type>[0-9a-zA-Z_]+)\s+(?P<keyspace>[0-9a-zA-Z-_]+)\s+(?P<table>[0-9a-zA-Z-_]+)', text)
        if not matches:
            return
        return matches.groupdict()

    def listsnapshots(self, async_=False):
        return self._run("nodetool listsnapshots", async_)
    
    def clear_table_snapshots(self, keyspace, table, async_):
        return (
            self._run(f"nodetool clearsnapshot -t {snapshot} -- {keyspace}", async_)
            for snapshot in self._find_table_snapshots(keyspace, table)
        )

    def find_table_snapshots(self, keyspace, table, async_=False):
        if async_:
            return self.async_find_table_snapshots(keyspace, table)
        table_snapshots = []
        results = [result for result in self.listsnapshots() if result]
        for result in results:
            output = result[0]
            for line in output.splitlines():
                snapshot = self._parse_snapshot(line)
                if snapshot and snapshot["keyspace"] == keyspace and snapshot["table"] == table:
                    table_snapshots.append(snapshot["name"])
        self._logger.info(f"Found snapshots: {table_snapshots}")
        return table_snapshots
    
    async def async_find_table_snapshots(self, keyspace, table):
        table_snapshots = []
        results = [result for result in await self.listsnapshots(async_=True) if result]
        for result in results:
            self._logger.info(f"res: {result}")
            output = result[0]
            for line in output.splitlines():
                snapshot = self._parse_snapshot(line)
                if snapshot and snapshot["keyspace"] == keyspace and snapshot["table"] == table:
                    table_snapshots.append(snapshot["name"])
        self._logger.info(f"Found snapshots: {table_snapshots}")
        return table_snapshots

    def _parse_snapshot(self, text):
        matches = re.match('(?P<name>[0-9a-zA-Z-_]+)\s+(?P<keyspace>[0-9a-zA-Z_]+)\s+(?P<table>[0-9a-zA-Z-_]+)', text)
        return matches.groupdict() if matches else None

    def repair_table(self, keyspace, table, async_=False):
        return self._run(f'nodetool repair -pr {keyspace} {table}', async_)
    
    def cleanup_table(self, keyspace, table, async_=False):
        return self._run(f'nodetool cleanup {keyspace} {table}', async_)
    
    def compact_table(self, keyspace, table, async_=False):
        return self._run(f'nodetool compact {keyspace} {table}', async_)

    def cqlsh(self, command, async_=False):
        return self._run(f"cqlsh {self._host} -e '{command}'", async_)

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
    elif args.command == "flush":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.flush_table(args.keyspace, args.table)
    elif args.command == "compactionstats":
        node.compactionstats()
    elif args.command == "find-table-compactions":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.find_table_compactions(args.keyspace, args.table)
    elif args.command == "stop-table-compactions":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.stop_table_compactions(args.keyspace, args.table)
    elif args.command == "stop-compaction":
        if not args.compaction_id:
            raise argparse.error("Compaction id should be specified!")
        node.stop_compaction(args.compaction_id)
    elif args.command == "listsnapshots":
        node.listsnapshots()
    elif args.command == "find-table-snapshots":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.find_table_snapshots(args.keyspace, args.table)
    elif args.command == "clear-table-snapshots":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.clear_table_snapshots(args.keyspace, args.table)
    elif args.command == "repair-table":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.repair_table(args.keyspace, args.table)
    elif args.command == "cleanup-table":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.cleanup_table(args.keyspace, args.table)
    elif args.command == "compact-table":
        if not all([args.keyspace, args.table]):
            raise argparse.error("Keyspace and table should be specified!")
        node.compact_table(args.keyspace, args.table)
    elif args.command == "cqlsh":
        if not args.cql_command:
            raise argparse.error("CQL command should be specified!")
        node.cqlsh(args.cql_command)
    else:
        node.run(args.command)
