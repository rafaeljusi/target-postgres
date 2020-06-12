import io
import sys

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

def main(config, input_stream=None):
    with psycopg2.connect(
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
    ) as connection:
        postgres_target = PostgresTarget(
            connection,
            postgres_schema=config.get('schema', 'public'),
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables'),
            add_upsert_indexes=config.get('add_upsert_indexes', True),
            before_run_sql=config.get('before_run_sql'),
            after_run_sql=config.get('after_run_sql'),
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, postgres_target, config=config)
        else:
            target_tools.main(postgres_target, config=config)


def cli():
    args = parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
