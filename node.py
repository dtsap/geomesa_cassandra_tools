import re
import time


class Node:

    def __init__(self, remote):
        self._remote = remote
    
    def info(self):
        return self._remote.run("nodetool info")

    def status(self):
        return self._remote.run("nodetool status")

    def is_active(self):
        return bool(
            re.search(
            "[\w\s\S]*Gossip[\w\s\S]*true[\w\s\S]*Thrift[\w\s\S]*true[\w\s\S]*Transport[\w\s\S]*true[\w\s\S]*",
            self.info()[0]
        ))

    def restart(self):
        self._remote.sudo("systemctl stop cassandra")
        self._remote.sudo("systemctl start cassandra")
        start_time = time.time()
        while time.time() - start_time < 300:
            if self.is_active():
                return True
            time.sleep(2)
        self._remote.disconnect()
        raise TimeoutError("TimeOut occurred! Couldn't restart the node!")

    def __del__(self):
        self._remote.disconnect()
