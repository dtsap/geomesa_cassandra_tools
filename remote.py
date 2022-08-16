"""Package to run remote commands."""
from time import sleep
import paramiko
import logging


class Connection:

    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password


def get_default_logger():
    logger = logging.getLogger()
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel("DEBUG")
    return logger


class Remote:

    def __init__(self, host, port, username, password, logger):
        self._connection = Connection(host, port, username, password)
        self._logger = logger or get_default_logger()
        self._client = paramiko.SSHClient()

    def connect(self):
        """
        Connect to the remote host.

        :param kwargs: Other parameters for the paramiko.SSHClient `connect` method.
        """
        if not self.is_connected():
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
            self._logger.debug(f"Connecting on {self._connection.host}...")
            self._client.connect(
                hostname=self._connection.host, 
                port=self._connection.port,
                username=self._connection.username,
                password=self._connection.password
            )

    def is_connected(self):
        return self._client.get_transport() is not None and self._client.get_transport().is_active()

    def disconnect(self):
        """
        Disconnect from the remote host.
        """
        if self._client:
            self._logger.debug(f"Disconnecting from {self._connection.host}...")
            return self._client.close()

    @classmethod
    def _write_stream(cls, stream, text):
        """
        Write text to stream.

        :param stream: The stream to write to.
        :param text: The text to write.
        """
        if not text:
            return
        stream.write(text)
        stream.flush()
        sleep(1)

    @classmethod
    def _read_stream(cls, stream):
        """
        Read from stream.

        :param stream: The stream to read from.
        :return: The stream contents.
        """
        stream.flush()
        return stream.read().decode('utf-8')

    def sudo(self, command, input_text=None):
        """
        Run command with sudo.

        :param command: The command to run.
        :param password: The password of the sudo user.
        :param input_text: The input text of the command (if needed by the command).
        :param kwargs: Other parameters of paramiko.SSHClient() `exec_command` method.
        :return: The output and the error of the command.
        """
        self.connect()

        if not command.startswith("sudo"):
            command = "sudo -S -p '' " + command

        self._logger.debug(command)
        stdin, stdout, stderr = self._client.exec_command(command)

        if self._connection.password and not self._connection.password.endswith('\n'):
            password = '{}\n'.format(self._connection.password)

        self._write_stream(stdin, password)

        if input_text is not None:
            self._write_stream(stdin, input_text)

        stdin.channel.shutdown_write()
        output = self._read_stream(stdout)
        error = self._read_stream(stderr)
        return output, error

    def run(self, command, input_text=None, sudo=False):
        """
        Run command.

        :param command: The command to run.
        :param input_text: The input text (if needed by the command).
        :param sudo: Determine to run with sudo.
        :param password: The sudo password (if sudo is true).
        :param kwargs: Other parameters of paramiko.SSHClient() `exec_command` method.
        :return: The output and the error of the command.
        """
        self.connect()

        if sudo:
            return self.sudo(command=command, input_text=input_text)

        self._logger.debug(command)
        stdin, stdout, stderr = self._client.exec_command(command)

        if input_text is not None:
            self._write_stream(stdin, input_text)

        stdin.channel.shutdown_write()
        output = self._read_stream(stdout)
        error = self._read_stream(stderr)
        return output, error

    def __del__(self):
        """ Called when the instance is about to be destroyed. 
                
        The connection has to be closed here.
        """
        self.disconnect()
