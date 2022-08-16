
import json

from node import Node
from remote import Remote


class Cluster:

    def __init__(self, nodes_file):
        self._nodes_file = nodes_file
        self._nodes = None
        self._load_nodes()


    def _load_nodes(self):
        with open(self._nodes_file) as fp:
            self._nodes = [
                Node(
                    Remote(
                        item["host"],
                        item["port"],
                        item["user"],
                        item["password"],
                        None
                    )
                )
                for item in json.load(fp).values()
            ]

    def restart(self):
        for node in self._nodes:
            node.restart()
            node.disconnect()

    def is_up(self):
        return all([node.is_active() for node in self._nodes])
