import argparse
import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import asyncssh


def parse_args():
    """Parse the script arguments.

    :return: The parsed args
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Run command on a remote.")
    parser.add_argument("command", help="The command to run")
    parser.add_argument("-r", "--remote", help="The remote name", required=False)
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


async def async_run_command(command, host, port, username, password, logger):
    """Run command on a remote asynchronously.

    :param str command: The command
    :param str host: The remote host
    :param int port: The remote port
    :param str username: The remote username
    :param str password: The remote password
    :param logging.Logger logger: The logger object
    """
    async with asyncssh.connect(host=host, port=port, username=username, password=password) as connection:
        result = await connection.run(command)
        logger.info(result)


def run_command(command, host, port, username, password, logger):
    """Run command on a remote.

    :param str command: The command
    :param str host: The remote host
    :param int port: The remote port
    :param str username: The remote username
    :param str password: The remote password
    :param logging.Logger logger: The logger object
    """
    asyncio.get_event_loop().run_until_complete(
        async_run_command(
            command, 
            host, 
            port, 
            username, 
            password,
            logger
        )
    )


async def async_run_command_on_remote(command, remote_name, logger):
    """Run command on a pre-defined remote asynchronously.

    :param str command: The command
    :param str remote_name: The remote name
    :param logging.Logger logger: The logger object
    """
    remote = get_remote(remote_name)
    return await async_run_command(command, remote["host"], remote["port"], remote["user"], remote["password"], logger)
        

def run_command_on_remote(command, remote_name, logger):
    """Run command on pre-defined remote.

    :param str command: The command
    :param str remote_name: The remote name
    :param logging.Logger logger: The logger object
    """
    asyncio.get_event_loop().run_until_complete(
        async_run_command_on_remote(
            command,
            remote_name,
            logger
        )
    )


def read_remotes():
    """Read remotes.

    :return: The remotes
    :rtype: list[dict]
    """
    with open("remotes.json") as fp:
        return json.load(fp)


def get_remote(name):
    """Return remote by name.

    :param str name: The remote name
    :return: The remote
    :rtype: dict
    """
    return read_remotes().get(name, None)


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
    if args.remote:
        run_command_on_remote(
            args.command,
            args.remote,
            setup_logger(args.log_level, args.log_file, args.error_log_file)
        )
    else:
        run_command(
            args.command,
            args.host,
            args.port,
            args.username,
            args.password,
            setup_logger(args.log_level, args.log_file, args.error_log_file)
        )
