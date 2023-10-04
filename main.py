import argparse
import configparser
from datetime import datetime
import json
import requests
from time import sleep
import sys
import math
from sql_metadata import Parser
from os import makedirs
import os
import sqlparse
import ruamel.yaml
import logging
import re
import ast

class DremioConfig:
    #TODO:
    # Permissions
    # Reflections
    def __init__(self, config):
        self.dremio_type = config[config_section]['type']
        self.username = config[config_section]['username']
        self.password = config[config_section]['password']
        self.project_name = config[config_section]['project_name']

        if self.dremio_type == 'cloud':
            self.project_id = config[config_section]['project_id']
        else:
            self.project_id = None

        self.output = config[config_section]['output']
        self.schemas = []
        self.view_query = config[config_section]['view_query']
        self.view_filter = config[config_section]['view_filter']
        self.table_query = config[config_section]['table_query']
        self.table_filter = config[config_section]['table_filter']

        # create url string
        if config[config_section]['ssl'] == 'true':
            self.url = 'https://'
        else:
            self.url = 'http://'

        self.url += config[config_section]['host'] + ":" + config[config_section]['port']

        # create the header
        self.headers = authenticate(self)

        # Dremio reserved words
        self.dremio_reserved = {'abs', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array',
                       'array_max_cardinality', 'as', 'asensitivelo', 'asymmetric', 'at', 'atomic', 'authorization',
                       'avg', 'begin', 'begin_frame', 'begin_partition', 'between', 'bigint', 'binary', 'bit', 'blob',
                       'boolean', 'both', 'by', 'call', 'called', 'cardinality', 'cascaded', 'case', 'cast', 'ceil',
                       'ceiling', 'char', 'char_length', 'character', 'character_length', 'check', 'classifier',
                       'clob', 'close', 'coalesce', 'collate', 'collect', 'column', 'commit', 'condition', 'connect',
                       'constraint', 'contains', 'convert', 'corr', 'corresponding', 'count', 'covar_pop',
                       'covar_samp', 'create', 'cross', 'cube', 'cume_dist', 'current', 'current_catalog',
                       'current_date', 'current_default_transform_group', 'current_path', 'current_role',
                       'current_row', 'current_schema', 'current_time', 'current_timestamp',
                       'current_transform_group_for_type', 'current_user', 'cursor', 'cycle', 'date', 'day',
                       'deallocate', 'dec', 'decimal', 'declare', 'default', 'define', 'delete', 'dense_rank',
                       'deref', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct', 'double', 'drop',
                       'dynamic', 'each', 'element', 'else', 'empty', 'end', 'end-exec', 'end_frame', 'end_partition',
                       'equals', 'escape', 'every', 'except', 'exec', 'execute', 'exists', 'exp', 'explain', 'extend',
                       'external', 'extract', 'false', 'fetch', 'filter', 'first_value', 'float', 'floor', 'for',
                       'foreign', 'frame_row', 'free', 'from', 'full', 'function', 'fusion', 'get', 'global', 'grant',
                       'group', 'grouping', 'groups', 'having', 'hold', 'hour', 'identity', 'import', 'in',
                       'indicator', 'initial', 'inner', 'inout', 'insensitive', 'insert', 'int', 'integer',
                       'intersect', 'intersection', 'interval', 'into', 'is', 'join', 'lag', 'language', 'large',
                       'last_value', 'lateral', 'lead', 'leading', 'left', 'like', 'like_regex', 'limit', 'ln',
                       'local', 'localtime', 'localtimestamp', 'lower', 'match', 'matches', 'match_number',
                       'match_recognize', 'max', 'measures', 'member', 'merge', 'method', 'min', 'minute', 'mod',
                       'modifies', 'module', 'month', 'more', 'multiset', 'national', 'natural', 'nchar', 'nclob',
                       'new', 'next', 'no', 'none', 'normalize', 'not', 'nth_value', 'ntile', 'null', 'nullif',
                       'numeric', 'occurrences_regex', 'octet_length', 'of', 'offset', 'old', 'omit', 'on', 'one',
                       'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay', 'parameter',
                       'partition', 'pattern', 'per', 'percent', 'percentile_cont', 'percentile_disc', 'percent_rank',
                       'period', 'permute', 'portion', 'position', 'position_regex', 'power', 'precedes', 'precision',
                       'prepare', 'prev', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive',
                       'ref', 'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept',
                       'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'regr_syy', 'release', 'reset', 'result',
                       'return', 'returns', 'revoke', 'right', 'rollback', 'rollup', 'row', 'row_number', 'rows',
                       'running', 'savepoint', 'scope', 'scroll', 'search', 'second', 'seek', 'select', 'sensitive',
                       'session_user', 'set', 'minus', 'show', 'similar', 'skip', 'smallint', 'some', 'specific',
                       'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static',
                       'stddev_pop', 'stddev_samp', 'stream', 'submultiset', 'subset', 'substring', 'substring_regex',
                       'succeeds', 'sum', 'symmetric', 'system', 'system_time', 'system_user', 'table', 'tablesample',
                       'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute', 'tinyint', 'to', 'trailing',
                       'translate', 'translate_regex', 'translation', 'treat', 'trigger', 'trim', 'trim_array',
                       'true', 'truncate', 'uescape', 'union', 'unique', 'unknown', 'unnest', 'update', 'upper',
                       'upsert', 'user', 'using', 'value', 'values', 'value_of', 'var_pop', 'var_samp', 'varbinary',
                       'varchar', 'varying', 'versioning', 'when', 'whenever', 'where', 'width_bucket', 'window',
                       'with', 'within', 'without', 'year'}


def authenticate(self):
    if self.dremio_type == 'cloud':
        # set cloud header
        headers = {
            'Authorization': f'Bearer {self.password}',
            'Content-Type': 'application/json'
        }
        return headers
    else:

        # follow software auth path
        payload = json.dumps({
            "userName": f"{self.username}",
            "password": f"{self.password}"
        })
        headers = {
            'Content-Type': 'application/json'
        }
        url = f"{self.url}/apiv2/login"

        response = requests.request("POST", url, headers=headers, data=payload)

        # if valid response
        if response.status_code == 200:
            token = response.json()['token']
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            return headers
        else:
            print(f'{datetime.now()} - failed to authenticate')
            print(f'{datetime.now()} - {response}')
            sys.exit(1)


def get_job(self, id):
    if self.dremio_type == 'cloud':
        url = f"{self.url}/v0/projects/{self.project_id}/job/{id}"
    else:
        url = f"{self.url}/api/v3/job/{id}"

    response = requests.request("GET", url, headers=self.headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'{datetime.now()} - Bad response when getting job {id}')
        print(response)
        sys.exit(1)


def get_results(self, id, offset):
    if self.dremio_type == 'cloud':
        url = f"{self.url}/v0/projects/{self.project_id}/job/{id}/results?offset={offset}&limit=500"
    else:
        url = f"{self.url}/api/v3/job/{id}/results?offset={offset}&limit=500"

    response = requests.request("GET", url, headers=self.headers)
    if response.status_code == 200:
        return response.json()['rows']
    else:
        print(f'{datetime.now()} - Bad response when getting job {id}')
        print(response)
        sys.exit(1)


def execute_query_rest(self, query):
    if self.dremio_type == 'cloud':
        url = f"{self.url}/v0/projects/{self.project_id}/sql"
    else:
        url = f"{self.url}/api/v3/sql"

    payload = json.dumps({
        "sql": query
    })

    response = requests.request("POST", url, headers=self.headers, data=payload)

    if response.status_code == 200:
        return response.json()['id']
    else:
        print(f'{datetime.now()} - Bad response when submitting sql {response.status_code}')
        print(response)
        sys.exit(1)


def execute_query(self, query):
    # run the query via rest (arrow flight has too many issues)
    job_id = execute_query_rest(self, query)

    # pull job status
    # possible job statuses NOT_SUBMITTED, STARTING, RUNNING, COMPLETED,
    # CANCELED, FAILED, CANCELLATION_REQUESTED, PLANNING, PENDING,
    # METADATA_RETRIEVAL, QUEUED, ENGINE_START, EXECUTION_PLANNING, INVALID_STATE

    valid_status = ['RUNNING', 'STARTING', 'NOT_SUBMITTED', 'PLANNING',
                    'PENDING', 'METADATA_RETRIEVAL', 'QUEUED', 'ENGINE_START', 'EXECUTION_PLANNING',
                    'COMPLETED']

    while True:
        job = get_job(self, job_id)
        job_state = job['jobState']
        if job_state in valid_status:
            if job_state == 'COMPLETED':
                print(f'{datetime.now()} - job state: {job_state}')
                print(f'{datetime.now()} - job_id: ' + job_id)
                calls = math.ceil(job['rowCount'] / 500)
                x = 0
                results = []
                while x < calls:
                    offset = x * 500
                    results = results + get_results(self, job_id, offset)
                    x += 1
                return results
            else:
                print(f'{datetime.now()} - job state: {job_state}')
                print(f'{datetime.now()} - job_id: ' + job_id)
                sleep(5)

        else:
            print(f'{datetime.now()} - job state not valid')
            print(f'{datetime.now()} - job state: {job_state}')
            print(f'{datetime.now()} - job_id: {job_id}')
            sys.exit(1)


def get_views(self):
    query = self.view_query
    views = execute_query(self, query)

    self.views = views


def get_filtered_views(self):
    query = f'{self.view_query} {self.view_filter}'
    views = execute_query(self, query)

    self.filtered_views = views


def get_tables(self):
    query = self.table_query
    tables = execute_query(self, query)

    self.tables = tables


def get_filtered_tables(self):
    query = f'{self.table_query} {self.table_filter}'
    tables = execute_query(self, query)

    self.filtered_tables = tables


def contains_non_alphanumeric(input_string):
    # Define a regular expression pattern to match non-alphanumeric characters
    pattern = r'[^a-zA-Z0-9_]'

    # Use the re.search() function to find the first match
    match = re.search(pattern, input_string)

    # If a match is found, return True (contains non-alphanumeric characters)
    # Otherwise, return False (only contains alphanumeric characters)
    return bool(match)


def build_parent_list(parent_path):
    # save the quoted path and unquoted path
    path_list = ast.literal_eval('[' + ', '.join(['"' + item.strip() + '"' for item in parent_path[1:-1].split(',')]) + ']')
    quoted_path_list = []
    for path in path_list:
        if contains_non_alphanumeric(path):
            quoted_path_list.append(f'"{path}"')
        else:
            quoted_path_list.append(path)
    quoted_path = ".".join(quoted_path_list)
    unquoted_path = ".".join(path_list)

    # Create list of untouched path and quoted path
    return {'unquoted': unquoted_path,
        'quoted': quoted_path}


def build_project_yaml(self):

    def create_schema(schema):
        return {'+schema': schema}

    def create_nested_dicts(data, keys):
        current_dict = data
        for key in keys:
            current_dict = current_dict.setdefault(key, {})
        current_dict.update(create_schema('.'.join(keys)))

    data = {}
    for item in self.schemas:
        # keys = re.findall(r'"(.*?)"', item)
        keys = item
        create_nested_dicts(data, keys)

    # Load the existing YAML file
    file_path = self.output + '/' + self.project_name +'/dbt_project.yml'

    # Load the existing YAML content while preserving formatting
    yaml = ruamel.yaml.YAML()
    with open(file_path, 'r') as file:
        existing_data = yaml.load(file)

    # Append the new data to the existing data
    models = {'models': {self.project_name: data}}
    existing_data.update(models)

    # Write the updated data back to the YAML file while maintaining formatting
    with open(file_path, 'w') as file:
        yaml.dump(existing_data, file)


def build_source_yaml(self, database, schema, table):
    pass


def build_model(self):
    # build list of sources
    source_list = []

    for table in self.tables:
        source_list.append({'source': build_parent_list(table['path']),
                            'type': 'table'})

    for view in self.views:
        source_list.append({'source': build_parent_list(view['path']),
                            'type': 'view'})
        path_list = ast.literal_eval(
            '[' + ', '.join(['"' + item.strip() + '"' for item in view['path'][1:-1].split(',')]) + ']')
        schema = path_list[0:-1]
        if schema not in self.schemas:
            self.schemas.append(schema)

    for view in self.filtered_views:
        model_path = self.output + "/" + self.project_name + "/models/" + "/".join(view['path'].split(', ')[0:-1])\
            .replace('[', '').replace(']', '')
        model_name = model_path + "/" + \
                     "_".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '') + \
                     "_" + view['view_name'] + '.sql'

        sql_obj = Parser(view['sql_definition'])
        new_query = sql_obj.query

        for query_table in sql_obj.tables:
            table_type = None
            full_table = None
            # Check if the context should be used, dremio always defaults to context source.
            if view['sql_context']:
                context_table = view['sql_context'] + "." + query_table
            else:
                context_table = None

            for source_table in source_list:
                if context_table == source_table['source']['unquoted']:
                    full_table = source_table['source']['quoted']
                    table_type = source_table['type']
                    break
                elif query_table == source_table['source']['unquoted']:
                    full_table = source_table['source']['quoted']
                    table_type = source_table['type']
                    break

            if full_table != None:
                #split back into parts
                table_parts = re.split(r'\.(?=(?:(?:[^"]*"){2})*[^"]*$)', full_table)

                database = table_parts[0]
                table = table_parts[-1]
                dbt_ref = table.replace('"', '').replace('.', '_')

                for value in self.dremio_reserved:
                    # Create a regular expression pattern that matches the reserved word surrounded by backticks
                    pattern = rf'`({re.escape(value)})`'

                    # Replace using re.sub() with the pattern
                    new_query = re.sub(pattern, r'"\1"', new_query)

                new_query = new_query.replace("`", '')

                if table_type == 'view':
                    ref = "{{ ref('" + dbt_ref + "') }}"
                    new_query = new_query.replace(query_table, ref)
                elif table_type == 'table':
                    ref = "{{ source('" + database + "','" + table + "') }}"
                    new_query = new_query.replace(query_table, ref)
                else:
                    print(f'{query_table} failed to match in {model_name}')

        # format sql
        final_sql = sqlparse.format(new_query, reindent=True)

        # create the new directories as needed
        is_exist = os.path.exists(model_path)
        if not is_exist:
            makedirs(model_path)

        # write the new model file
        with open(model_name, "w") as file:
            file.write(final_sql)


if __name__ == "__main__":
    # parse input arguments for config file location
    parser = argparse.ArgumentParser(
        prog='dremio-dbt-exporter',
        description='exports an existing dremio environment to a dbt model')
    parser.add_argument('-config', default='config.ini')
    parser.add_argument('-target')

    # read args
    args = parser.parse_args()
    config_file = args.config
    config_section = args.target

    # get the config properties
    config = configparser.ConfigParser()
    config.read(config_file)

    # set config
    dremio_conn = DremioConfig(config)
    print("Getting tables")
    get_tables(dremio_conn)
    print("Getting views")
    get_views(dremio_conn)
    print("Getting filtered tables")
    get_filtered_tables(dremio_conn)
    print("Getting filterd views")
    get_filtered_views(dremio_conn)
    print("building model")
    build_model(dremio_conn)
    print("building project_yaml")
    build_project_yaml(dremio_conn)


