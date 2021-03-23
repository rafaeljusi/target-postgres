import io
import sys

import json

import argparse
from singer import utils
import psycopg2

from target_postgres.postgres import MillisLoggingConnection, PostgresTarget
from target_postgres import target_tools

REQUIRED_CONFIG_KEYS = [
    'database'
]


def parse_args(required_config_keys):
    '''Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '--input',
        help='Inpit file')

    parser.add_argument(
        '--output',
        help='Output file')

    parser.add_argument(
        '-t', '--test',
        action='store_true',
        help='Test connection with specified config')

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = utils.load_json(args.config)
    if args.input:
        setattr(args, 'input_path', args.input)
        sys.stdin = open(args.input, 'r')
    if args.output:
        setattr(args, 'output_path', args.output)
        sys.stdout = open(args.output, 'w')
    

    utils.check_config(args.config, required_config_keys)

    return args

def get_connection(config):
    return psycopg2.connect(
            connection_factory=MillisLoggingConnection,
            host=config.get('host', 'localhost'),
            port=config.get('port', 5432),
            dbname=config.get('database'),
            user=config.get('username'),
            password=config.get('password'),
            sslmode=config.get('sslmode'),
            sslcert=config.get('sslcert'),
            sslkey=config.get('sslkey'),
            sslrootcert=config.get('sslrootcert'),
            sslcrl=config.get('sslcrl')
    )

def get_target(connection, config):
        return PostgresTarget(
            connection,
            postgres_schema=config.get('schema', 'public'),
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables'),
            add_upsert_indexes=config.get('add_upsert_indexes', True),
            before_run_sql=config.get('before_run_sql'),
            after_run_sql=config.get('after_run_sql'),
        )

def test(config):
    result = {}
    try:
        with get_connection(config) as connection:
            postgres_target = get_target(connection, config)

            with postgres_target.conn.cursor() as cur:
                cur.execute("SELECT 1")
                result["connected"] = True
    except Exception as e:
        result["connected"] = False
        result["message"] = '{}'.format(*e.args)

    json.dump(result, sys.stdout, indent=2)

def main(config, input_stream=None):
    with get_connection(config) as connection:
        postgres_target = get_target(connection, config)

        if input_stream:
            target_tools.stream_to_target(input_stream, postgres_target, config=config)
        else:
            target_tools.main(postgres_target, config=config)

    print('finished')


def cli():
    args = parse_args(REQUIRED_CONFIG_KEYS)

    if args.test:
        test(args.config)
    else:
        main(args.config)
