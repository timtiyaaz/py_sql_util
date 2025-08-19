import os
import csv
import json
from abc import ABC, abstractmethod
from typing import Optional
import mysql.connector as mysql # type: ignore
import singlestoredb as s2 # type: ignore
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Connection(ABC):
    def __init__(self, host: str, user: str, password: str, database: str, port: str):
        self.host: str = host
        self.user: str = user
        self.password: str = password
        self.database: str = database
        self.port: str = port
        self.connection: Optional[mysql.MySQLConnection] = None
        self.cursor: Optional[mysql.MySQLCursorDict] = None

    @classmethod
    def from_dict(cls, conn_details):
        return cls( 
            conn_details['host'],
            conn_details['user'],
            conn_details['password'],
            conn_details['database'],
            conn_details['port']
        )
    
    @abstractmethod
    def connect(self):
        pass 

    def execute_and_fetchall(self, sql, params=()):
        start = time.perf_counter()
        if len(params) >= 1:
            self.cursor.execute(sql, params) # type: ignore
        else:
            self.cursor.execute(sql) # type: ignore
        end = time.perf_counter()
        elapsed = end - start
        
        logging.info(f'Query execution completed after {elapsed:.4} seconds')
        return self.cursor.fetchall() # type: ignore

class MySqlConnection(Connection):
    def __init__(self, host, user, password, database, port):
        super().__init__(host, user, password, database, port)

    def connect(self): # type: ignore
        self.connection = mysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        self.cursor = self.connection.cursor(dictionary=True) # type: ignore
        return self

class S2Connection(Connection):
    def __init__(self, host, user, password, database, port):
        super().__init__(host, user, password, database, port)

    def connect(self): # type: ignore
        self.connection = s2.connect(
            results_type='dict',
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        self.cursor = self.connection.cursor() # type: ignore
        return self

def get_db_credentials(conn_name):
    conn_details = {}
    with open('db_credentials.json', 'r') as jf:
        conn_details = json.load(jf)
    return conn_details[conn_name]

def to_csv(dest_dir, dest_file_name, result_set_as_list_of_dicts):
    extension = 'csv'
    if dest_dir and not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    if len(result_set_as_list_of_dicts) > 0:
        with open(f'{dest_dir}/{dest_file_name}.{extension}', mode='w', newline='') as out:
            writer = csv.DictWriter(out, fieldnames=result_set_as_list_of_dicts[0].keys())
            writer.writeheader()
            writer.writerows(result_set_as_list_of_dicts)

def from_csv(dest_dir, dest_file_name):
    with open(f'{dest_dir}/{dest_file_name}', mode='r', newline='') as csv_file:
        return list(csv.DictReader(csv_file))

def to_json(dest_dir, dest_file_name, result_set_as_list_of_dicts):
    extension = 'json'
    with open(f'{dest_dir}/{dest_file_name}.{extension}', 'w') as out:
        json.dump(result_set_as_list_of_dicts, out, indent=4)

def get_query_from_file(dest_dir, file_name):
    with open(f'{dest_dir}/{file_name}', 'r') as sql_file:
        return sql_file.read()
    
def clear_all_results(dest_dir):
    for filename in os.listdir(dest_dir):
        file_path = os.path.join(dest_dir, filename)

        extension = filename.split('.')[-1].lower()
        if os.path.isfile(file_path) and (extension == 'csv' or extension == 'json'):
            os.remove(file_path)


def close_sql_objects(objects: list[Connection]=[]):
    if len(objects) > 0:
        for object in objects:
            object.connection.close() # type: ignore
            object.cursor.close() # type: ignore