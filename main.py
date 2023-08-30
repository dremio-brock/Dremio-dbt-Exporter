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
import logging


class DremioConfig:
    def __init__(self, config):
        self.dremio_type = config[config_section]['type']
        self.username = config[config_section]['username']
        self.password = config[config_section]['password']
        self.project_id = config[config_section]['project_id']
        self.output = config[config_section]['output']

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
            "userName": "{{user}}",
            "password": "{{pass}}"
        })
        headers = {
            'Content-Type': 'application/json'
        }
        url = f"{self.url}/apiv2/login"

        response = requests.request("POST", url, headers=headers, data=payload)

        # if valid response
        if response.status_code == '200':
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
    if self.dremio_type == 'cloud':
        query = 'select * from sys.project."views"'
    else:
        query = 'select * from sys."views"'
    views = execute_query(self, query)

    self.views = views


def get_tables(self):
    if self.dremio_type == 'cloud':
        query = 'select * from sys.project."tables"'
    else:
        query = 'select * from sys."tables"'
    tables = execute_query(self, query)

    self.tables = tables


def build_model(self):
    # build list of sources
    source_list = []
    for table in self.tables:
        source_list.append(".".join(table['path'].replace('[', '').replace(']', '').split(', ')))

    for view in self.views:
        model_path = self.output + "/" + "/".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '')
        model_name = model_path + "/" + \
                     "_".join(view['path'].split(', ')[0:-1]).replace('[', '').replace(']', '') + \
                     "_" + view['view_name'] + '.sql'
        # todo: use the context and validate the fully qualified path. We may need to revise the SQL with the correct
        #  parents
        # todo: look at CTE statements
        sql_obj = Parser(view['sql_definition'])
        tables = Parser(view['sql_definition']).tables

        # change quotes to dremio formatted qoutes
        new_sql = sql_obj.query.replace('`', '"')

        # loop through fully qualified list of tables and replace with source / reference identifiers
        for fq_table in tables:
            if fq_table in source_list:
                # todo: use database, schema, table for source yaml generation
                database = fq_table.split('.')[0]
                schema = fq_table.split('.')[1:-1]
                table = fq_table.split('.')[-1]

                ref = "{{ source({'" + database + "','" + table + "') } }}"
            else:
                ref = "{{ ref({'" + fq_table + "') } }}"


            # Add double qoutes where needed
            table_list_formatted = []
            for x in fq_table.split('.'):
                if ' ' in x or '-' in x or '_' in x:
                    table_list_formatted.append('"' + x + '"')
                else:
                    table_list_formatted.append(x)

            fq_table_formatted = ".".join(table_list_formatted)

            # replace old references
            new_sql = new_sql.replace(fq_table_formatted, ref)

        # format sql
        final_sql = sqlparse.format(new_sql, reindent=True)

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
        prog='ProgramName',
        description='What the program does',
        epilog='Text at the bottom of help')
    parser.add_argument('config', default='config.ini')
    parser.add_argument('target')

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
