import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re

import argparse
import asyncssh


parser = argparse.ArgumentParser(description="Remove a GeoMesa schema from Cassandra (geomesa-cassandra).")
parser.add_argument("-k", "--keyspace", help="the schema keyspace", required=True)
parser.add_argument("-c", "--catalog", help="The schema catalog", required=True)
parser.add_argument("-f", "--feature-name", help="The schema name", required=True)
parser.add_argument("-l", "--log-level", help="The log level", choices=["INFO", "ERROR", "DEBUG"], default="INFO")
args = parser.parse_args()


CURRENT_PATH = Path(os.path.abspath(__file__))
logger = logging.getLogger(__name__)


def remove_geomesa_schema(keyspace, catalog, schema):
    logger.info('Start removing tables of geo-schema ...')
    logger.info(f'Keyspace: {keyspace}')
    logger.info(f'Catalog: {catalog}')
    logger.info(f'Schema: {schema}')
    seed_node = '10.148.128.236'
    geomesa_tables = identify_schema_tables(seed_node, keyspace, catalog, schema)
    logger.info(f'Cassandra tables of schema: {", ".join(geomesa_tables)}')
    table_existence_states = tables_exist(seed_node, keyspace, geomesa_tables)
    logger.info(f'Checking tables existence ...')
    if not all(table_existence_states):
        not_existing_tables = [geomesa_tables[index] for index, state in enumerate(table_existence_states) if not state]
        raise Exception(f'Not found tables:{", ".join(not_existing_tables)}')
    logger.info('All tables exist!')
    logger.info('Removing tables from Cassandra...')
    for geomesa_table in geomesa_tables:
        remove_table(keyspace, geomesa_table)
    
    # logger.info('Deleting sft record from catalog ...')
    # delete_sft_from_catalog(seed_node, keyspace, catalog, schema)
    logger.info(f'Successfully finished removal of geo-schema {schema} of {keyspace} keyspace and {catalog} catalog!')


def identify_schema_tables(node, keyspace, catalog, schema):
    command = f'cqlsh {node} -e "SELECT value FROM {keyspace}.{catalog} where sft=\'{schema}\';exit;"'
    result = asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command), return_exceptions=True))
    results = [value.strip().lower() for value in result[0].stdout.split("\n")]
    return list(filter(lambda x: x.startswith(catalog), results))


def tables_exist(node, keyspace, tables):
    tasks = (run_command(node, f'cqlsh {node} -e "DESCRIBE {keyspace}.{table};"') for table in tables)
    results = asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    return [not result.stderr or not 'not found' in result.stderr for result in results]


def delete_sft_from_catalog(node, keyspace, catalog, schema):
    command = f'cqlsh {node} -e "DELETE FROM {keyspace}.{catalog} WHERE sft=\'{schema}\';"'
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command), return_exceptions=True))
    

def remove_table(keyspace, table):
    logger.info(f'Removing table: {keyspace}.{table}')
    nodes = get_remote_ips()
    seed_node = nodes[0]
    flush_table(nodes, keyspace, table)
    logger.info(75*'=')
    stop_compations_of_table(nodes, keyspace, table)
    logger.info(75*'=')
    truncate_table(seed_node, keyspace, table)
    logger.info(75*'=')
    clear_table_snapshots(nodes, keyspace, table)
    logger.info(75*'=')
    repair_table(nodes, keyspace, table)
    logger.info(75*'=')
    cleanup_table(nodes, keyspace, table)
    logger.info(75*'=')
    compact_table(nodes, keyspace, table)
    logger.info(75*'=')
    # drop_table(seed_node, keyspace, table)
    logger.info(f'Table {keyspace}.{table} has been removed!')

def flush_table(nodes, keyspace, table):
    command = f'nodetool flush -- {keyspace} {table}'
    tasks = (run_command(node, command) for node in nodes)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def stop_compations_of_table(nodes, keyspace, table):
    compactions = find_table_compactions(nodes, keyspace, table)
    for compaction in compactions:
        stop_compaction(compaction['node'], compaction['compaction_id'])


def find_table_compactions(nodes, keyspace, table):
    results = get_compaction_stats(nodes)
    compactions = []
    for result, node in zip(results, nodes):
        output = get_output_or_raise(result)
        for line in output.splitlines():
            compaction = parse_compaction(line)
            if compaction and compaction['keyspace'] == keyspace and compaction['table'] == table:
                compactions.append({
                    'node': node,
                    'compaction_id': compaction['id']
                })
    return compactions


def get_compaction_stats(nodes):
    command = f'nodetool compactionstats'
    tasks = (run_command(node, command) for node in nodes)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def parse_compaction(text):
    matches = re.match('(?P<id>[0-9a-zA-Z-_]+)\s+(?P<type>[0-9a-zA-Z_]+)\s+(?P<keyspace>[0-9a-zA-Z-_]+)\s+(?P<table>[0-9a-zA-Z-_]+)', text)
    if not matches:
        return
    return matches.groupdict()


def stop_compaction(node, compaction_id):
    command = f'nodetool stop -id {compaction_id}'
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command, raise_error=True)))
    

def truncate_table(node, keyspace, table):
    command = f'cqlsh {node} -e "CONSISTENCY ALL;TRUNCATE {keyspace}.{table};exit;"'
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command), return_exceptions=True))


def clear_table_snapshots(nodes, keyspace, table):
    snapshots = find_table_snapshots(nodes, keyspace, table)
    tasks = (run_command(snapshot['node'], f"nodetool clearsnapshot -t {snapshot['name']} -- {snapshot['keyspace']}") for snapshot in snapshots)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def find_table_snapshots(nodes, keyspace, table):
    results = list_snapshots(nodes)
    table_snapshots = []
    for result, node in zip(results, nodes):
        output = get_output_or_raise(result)
        for line in output.splitlines():
            snapshot = parse_snapshot(line)
            if snapshot and snapshot['keyspace'] == keyspace and snapshot['table'] == table:
                snapshot['node'] = node
                table_snapshots.append(snapshot)
    return table_snapshots


def list_snapshots(nodes):
    command = f'nodetool listsnapshots'
    tasks = (run_command(node, command) for node in nodes)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def parse_snapshot(text):
    matches = re.match('(?P<name>[0-9a-zA-Z-_]+)\s+(?P<keyspace>[0-9a-zA-Z_]+)\s+(?P<table>[0-9a-zA-Z-_]+)', text)
    return matches.groupdict() if matches else None


async def clear_table_snapshot(node, snapshot_name, keyspace):
    command = f"nodetool clearsnapshot -t {snapshot_name} -- {keyspace}"
    return run_command(node, command)


def repair_table(nodes, keyspace, table):
    command = f'nodetool repair -pr {keyspace} {table}'
    # return [
    #     asyncio.get_event_loop().run_until_complete(run_command(node, command)) 
    #     for node in nodes
    # ]
    tasks = (run_command(node, command) for node in nodes)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def cleanup_table(nodes, keyspace, table):
    command = f'nodetool cleanup {keyspace} {table}'
    tasks = (run_command(node, command) for node in nodes)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def compact_table(nodes, keyspace, table):
    command = f'nodetool compact {keyspace} {table}'
    tasks = (run_command(node, command) for node in nodes)
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))


def drop_table(node, keyspace, table):
    command = f'cqlsh {node} -e "DROP TABLE {keyspace}.{table};exit;"'
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command), return_exceptions=True))


async def run_command(host, command, raise_error=False):
    remote = get_remote(host)
    async with asyncssh.connect(host=remote["host"], port=remote["port"], username=remote["user"], password=remote["password"]) as connection:
        result = await connection.run(command)
        logger.debug(75*'-')
        logger.debug(host)
        logger.debug(len(host)*'*')
        logger.debug(command)
        logger.debug(f"Output: {result.stdout}")
        logger.debug(f"Error: {result.stderr}")
        if result.stderr:
            logger.error(f"Command: {command}")
            logger.error(f"Error: {result.stderr}")
            if raise_error:
                raise Exception(f'Command Error: {command}::{result.stderr}')
        return result


def get_remotes():
    with open("remotes.json") as fp:
        return json.load(fp)

def get_remote(name):
    return get_remotes().get(name, None)


def get_remote_ips():
    return [remote["host"] for name, remote in get_remotes().items()]


def get_output_or_raise(result):
    if isinstance(result, Exception):
        raise Exception(result)
    elif result.exit_status != 0:
        raise Exception(result.stderr)
    else:
        return result.stdout


def setup_logger(level):
    global logger
    filename, extension = os.path.splitext(__file__)
    file_handler = RotatingFileHandler(CURRENT_PATH.parent.joinpath(f'{filename}.log'),
                                       maxBytes=10000000,
                                       backupCount=0)
    error_file_handler = RotatingFileHandler(CURRENT_PATH.parent.joinpath(f'{filename}.error.log'),
                                       maxBytes=10000000,
                                       backupCount=0)    
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


def change_ttl(keyspace, catalog, schema, ttl):
    logger.info('Start setting the TTL to tables of geo-schema ...')
    logger.info(f'Keyspace: {keyspace}')
    logger.info(f'Catalog: {catalog}')
    logger.info(f'Schema: {schema}')
    seed_node = '10.148.128.236'
    geomesa_tables = identify_schema_tables(seed_node, keyspace, catalog, schema)
    for geomesa_table in geomesa_tables:
        set_table_ttl(seed_node, keyspace, geomesa_table, ttl)
    logger.info(f'The TTL has been set to {ttl} for schema {schema}!')


def set_table_ttl(node, keyspace, table, ttl):
    command = f'cqlsh {node} -e "ALTER TABLE {keyspace}.{table} WITH default_time_to_live = {ttl};"'
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command), return_exceptions=True))


def change_gc_grace_seconds(keyspace, catalog, schema, gc_grace_seconds):
    logger.info('Start setting the gc_grace_seconds to tables of geo-schema ...')
    logger.info(f'Keyspace: {keyspace}')
    logger.info(f'Catalog: {catalog}')
    logger.info(f'Schema: {schema}')
    seed_node = '10.148.128.236'
    geomesa_tables = identify_schema_tables(seed_node, keyspace, catalog, schema)
    for geomesa_table in geomesa_tables:
        set_table_gc_grace_seconds(seed_node, keyspace, geomesa_table, gc_grace_seconds)
    logger.info(f'The gc_grace_seconds has been set to {gc_grace_seconds} for schema {schema}!')


def set_table_gc_grace_seconds(node, keyspace, table, gc_grace_seconds):
    command = f'cqlsh {node} -e "ALTER TABLE {keyspace}.{table} WITH gc_grace_seconds = {gc_grace_seconds};"'
    return asyncio.get_event_loop().run_until_complete(asyncio.gather(run_command(node, command), return_exceptions=True))


if __name__ == '__main__':
    setup_logger(args.log_level)
    logger.info(f"Removing schema {args.feature_name} from catalog {args.catalog} of keyspace {args.keyspace}.")
    remove_geomesa_schema(args.keyspace, args.catalog, args.feature_name)
    # change_gc_grace_seconds(args.keyspace, args.catalog, args.feature_name, 1200)
