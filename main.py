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
        self.table_query = config[config_section]['table_query']

        # create url string
        if config[config_section]['ssl'] == 'true':
            self.url = 'https://'
        else:
            self.url = 'http://'

        self.url += config[config_section]['host'] + ":" + config[config_section]['port']

        # create the header
        self.headers = authenticate(self)


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


def get_tables(self):
    query = self.table_query
    tables = execute_query(self, query)

    self.tables = tables

def parse_tables_with_schema(query):
    #TODO: Add CTE support
    parsed = sqlparse.parse(query)
    tables = []
    table_alias = ''

    for statement in parsed:
        for token in statement.tokens:
            # if isinstance(token, sqlparse.sql.IdentifierList):
            #     for identifier in token.get_identifiers():
            #         full_table = ""
            #         for part in identifier.flatten():
            #             if part != identifier.get_alias():
            #                 full_table += part.value.strip()
            #         tables.append(full_table)
            if isinstance(token, sqlparse.sql.Identifier):
                full_table = ""
                token_list = list(token.flatten())
                token_length = len(token_list)
                for i, part in enumerate(token.flatten()):
                    if i != token_length - 1:
                        full_table += part.value
                    elif token.get_alias():
                        table_alias = full_table + part.value
                    else:
                        full_table += part.value
                        table_alias = full_table + part.value
                tables.append((full_table.strip(), table_alias.strip()))

    return tables

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
        # save the quoted path and unquoted path
        for table_path in table['path']:
            if '-' in table_path or '.' in table_path or '':
        source_list.append(
            [
                ".".join(table['path'].replace('[', '').replace(']', '').split(', ')),
                table['path']
            ]
        )

    for view in self.views:
        model_path = self.output + "/" + self.project_name + "/model/" + "/".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '')
        model_name = model_path + "/" + \
                     "_".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '') + \
                     "_" + view['view_name'] + '.sql'

        sql_obj = Parser(view['sql_definition'])
        context_table = None
        for query_table in sql_obj.tables:
            # Check if the context should be used, dremio always defaults to context source.
            for source_table in source_list:
                context_table = view['sql_context'] + "." + query_table
                if context_table == source_table[1]:
                    table = source_table[0]

            if view['sql_context'] + "." + query_table in source_list[1]:
                context_table = None


            database = None
            schema = None
            dbt_ref = None


def build_model_old(self):
    # build list of sources
    source_list = []
    for table in self.tables:
        source_list.append(ast.literal_eval(table['path']))


    for view in self.views:
        model_path = self.output + "/" + self.project_name + "/model/" + "/".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '')
        model_name = model_path + "/" + \
                     "_".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '') + \
                     "_" + view['view_name'] + '.sql'
        sql_obj = Parser(view['sql_definition'])

        # This does not work due to tables with . and - that require double quotes
        #tables = Parser(view['sql_definition']).tables

        query_tables = parse_tables_with_schema(view['sql_definition'].replace(")", "").replace("(", ""))

        new_query = view['sql_definition']

        # returns tuple with full path, full path + alias
        for query_table in query_tables:
            # Check if the context should be used, dremio always defaults to context source.
            if view['sql_context'] + "." + query_table[0] in source_list:
                context_table = view['sql_context'] + "." + query_table[0]
                table_parts = re.split(r'\.(?=(?:(?:[^"]*"){2})*[^"]*$)', context_table)
            else:
                table_parts = re.split(r'\.(?=(?:(?:[^"]*"){2})*[^"]*$)', query_table[0])

            database = table_parts[0]
            schema = '"' + '"."'.join(table_parts[0:-1]) + '"'
            table = table_parts[-1]
            dbt_ref = table.replace('"', '').replace('.', '_')

            if schema not in self.schemas:
                self.schemas.append(schema)
                ref = "{{ source('" + database + "','" + table + "') }}"
            else:
                ref = "{{ ref('" + dbt_ref + "') }}"

            # use query_table[0] to keep alias
            new_query = new_query.replace(query_table[0], ref)

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
        description='exports an existing dremio enviroment to a dbt model')
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
    get_tables(dremio_conn)
    get_views(dremio_conn)
    build_model(dremio_conn)
    build_project_yaml(dremio_conn)


