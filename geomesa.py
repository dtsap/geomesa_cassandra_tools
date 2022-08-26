"""Script including geomesa-cassandra utilities."""
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from node import NamedNode, Node

def parse_args():
    parser = argparse.ArgumentParser(description="GeoMesa-cassandra tools.")
    parser.add_argument("command", help="The command to run")
    parser.add_argument("-n", "--remote", help="The node remote", required=False)
    parser.add_argument("-k", "--keyspace", help="the schema keyspace", required=True)
    parser.add_argument("-c", "--catalog", help="The schema catalog", required=True)
    parser.add_argument("-f", "--feature-name", help="The schema name", required=False)
    parser.add_argument("-e", "--cql-command", help="The CQL command to run", required=False)
    parser.add_argument("-i", "--host", help="The remote hostname", required=False)
    parser.add_argument("-p", "--port", type=int, help="The remote port", required=False)
    parser.add_argument("-u", "--username", help="The remote username", required=False)
    parser.add_argument("-w", "--password", help="The remote password", required=False)
    parser.add_argument("-l", "--log-level", help="The logging level", choices=["INFO", "ERROR", "DEBUG"], default="INFO")
    parser.add_argument("--log-file", default=f"logs/{Path(__file__).stem}.log", help="The logging file")
    parser.add_argument("--error-log-file", default=f"logs/{Path(__file__).stem}.error.log", help="The error logging file")
    args = parser.parse_args()
    if args.remote and any([args.host, args.port, args.username, args.password]):
        parser.error("Only one of the remote and custom remote can be specified.")
    if not args.remote and not all([args.host, args.port, args.username, args.password]):
        parser.error("All custom remote fields should be specified.")
    return parser.parse_args()


class GeomesaNode(Node):
    
    def list_sfts(self, keyspace, catalog):
        result = self.cqlsh(
            f"SELECT sft FROM {keyspace}.{catalog};exit;"
        )
        sfts = list(set(
            sft.strip()
            for sft in result[0].split("\n")
            if sft.startswith("    ")
        ))
        self._logger.info(f"Found {len(sfts)} sfts: {sfts}")
        return sfts

    def find_schema_tables(self, keyspace, catalog, feature_name):
        result = self.cqlsh(
            f"SELECT value FROM {keyspace}.{catalog} where sft='{feature_name}';exit;"
        )
        all_tables = [value.strip().lower() for value in result[0].split("\n")]
        schema_tables = list(filter(lambda x: x.startswith(catalog), all_tables))
        self._logger.info(f"Found schema tables: {schema_tables}")
        return schema_tables

    def remove_sft_from_catalog(self, keyspace, catalog, feature_name):
        return self.cqlsh(
            f"DELETE FROM {keyspace}.{catalog} WHERE sft=\'{feature_name}\';"
        )


class NamedGeomesaNode(GeomesaNode):
    
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
    node = NamedGeomesaNode(
        args.remote, 
        setup_logger(args.log_level, args.log_file, args.error_log_file)
    ) if args.remote else GeomesaNode(
        args.host,
        args.port,
        args.username,
        args.password,
        setup_logger(args.log_level, args.log_file, args.error_log_file)
    )

    if args.command == "list-sfts":
        if not all([args.keyspace, args.catalog]):
            raise argparse.error("Keyspace and catalog should be specified!")
        node.list_sfts(args.keyspace, args.catalog)
    elif args.command == "find-schema-tables":
        if not all([args.keyspace, args.catalog, args.feature_name]):
            raise argparse.error("Keyspace, catalog and feature name should be specified!")
        node.find_schema_tables(args.keyspace, args.catalog, args.feature_name)
    elif args.command == "remove-sft-from-catalog":
        if not all([args.keyspace, args.catalog, args.feature_name]):
            raise argparse.error("Keyspace, catalog and feature name should be specified!")
        node.remove_sft_from_catalog(args.keyspace, args.catalog, args.feature_name)