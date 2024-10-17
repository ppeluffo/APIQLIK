#!/home/pablo/Spymovil/python/proyectos/APIQLIK/venv/bin/python3
"""
"""

import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

PGSQL_HOST = os.environ.get('PGSQL_HOST','127.0.0.1')
PGSQL_PORT = os.environ.get('PGSQL_PORT', '5434')
PGSQL_USER = os.environ.get('PGSQL_USER', 'admin')
PGSQL_PASSWD = os.environ.get('PGSQL_PASSWD','pexco599')
PGSQL_BD = os.environ.get('PGSQL_BD', 'bd_apiqlik')

BD_URL = f'postgresql+psycopg2://{PGSQL_USER}:{PGSQL_PASSWD}@{PGSQL_HOST}:{PGSQL_PORT}/{PGSQL_BD}'
# BD_URL = 'sqlite:///anep.sqlite'

class Bd:

    def __init__(self):
        self.engine = None
        self.conn = None
        self.connected = False
        self.response = ''
        self.status_code = 0
        self.url = f'postgresql+psycopg2://{PGSQL_USER}:{PGSQL_PASSWD}@{PGSQL_HOST}:{PGSQL_PORT}/{PGSQL_BD}'

    def get_engine(self):
        return self.engine
    
    def get_connector(self):
        return self.conn
    
    def close(self):
        self.conn.close()
        
    def connect(self):
        # Engine
        try:
            #self.engine = create_engine(url=self.url, echo=False, isolation_level="AUTOCOMMIT")
            self.engine = create_engine(url=self.url, echo=False )
        except SQLAlchemyError as err:
            print( f'CONNECT ERROR: Pgsql engine error, HOST:{PGSQL_HOST}:{PGSQL_PORT}, Err:{err}')
            self.connected = False
            return False 
        # Connection
        try:
            self.conn = self.engine.connect()
            #self.conn.autocommit = True
        except SQLAlchemyError as err:
            print( f'CONNECT ERROR: Pgsql connection error, HOST:{PGSQL_HOST}:{PGSQL_PORT}, Err:{err}')
            self.connected = False
            return False
        #
        self.connected = True
        return self.conn
        #

    def exec_sql(self, stmt, commit=False):
        # Ejecuta la orden sql.
        # Retorna un resultProxy o None
        if not self.connected:
            if not self.connect():
                print( f'EXEC_SQL: Pgsql connection error, HOST:{PGSQL_HOST}:{PGSQL_PORT}, Err:{err}')
                return False
        #
        try:
            query = text(stmt)
        except Exception as err:
            print( f'EXEC_SQL: Sql query error, HOST:{PGSQL_HOST}:{PGSQL_PORT}, Err:{stmt}')
            print( f'EXEC_SQL: Sql query exception, HOST:{PGSQL_HOST}:{PGSQL_PORT}, Err:{err}')
            return False
        #
        try:
            #print(sql)
            cursor = self.conn.execute(query)
        except Exception as err:
            print( f'EXEC_SQL: Sql exec error, HOST:{PGSQL_HOST}:{PGSQL_PORT}, Err:{err}')
            self.conn.rollback()
            return False
        if commit:
            self.conn.commit()
        #
        return cursor

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

        