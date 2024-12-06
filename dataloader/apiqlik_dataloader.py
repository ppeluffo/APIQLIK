#!/home/pablo/Spymovil/python/proyectos/APIQLIK/venv/bin/python3
'''
Loop infinito en que lee datos a travez de la API datos y los inserta en
la base pgsql local, en la tabla 'historicos'.
De este modo recrea la BD de Spymovil.
Otro proceso, 1 vez por hora lee la tabla de configuraciones y actualiza la local.
Mientras hallan datos, los lee cada 10 segundos, para no apretar al sistema.
'''

import sys
# Los 2 insert son porque un trabaja para el docker y otro localmente
sys.path.insert(0,'bdatos/')
sys.path.insert(0,'../bdatos/')
import os
import time
import signal
from multiprocessing import Process
import datetime as dt
from sqlalchemy import select, bindparam
from sqlalchemy.dialects.postgresql import insert
import requests
import time
from base_datos import Bd
import schemas as scm

SLEEP_TIME = int(os.environ.get('SLEEP_TIME',60))
APIDATOS_HOST = os.environ.get('APIDATOS_HOST','192.168.0.8')
APIDATOS_PORT = os.environ.get('APIDATOS_PORT','5300')
APIDATOS_USERKEY = os.environ.get('APIDATOS_USERKEY','L92HIJVRL7RJMP7EO9GF')

VERSION = 'R001 @ 2024-10-07'

class Dataloader:

    def __init__(self):

        self.bd = Bd()
        self.tables = scm

        self.l_datos = []
        self.l_datos_filtrados = []
        self.l_dlgid = []

        # Me conecto y creo/actualizo el metadata. Si las tablas no existen se crean en la BD.
        if self.bd.connect():
            self.tables.metadata.create_all(self.bd.get_engine())
            self.bd.close()
        
    def get_l_datos(self):
        '''
        Retorna la lista con todos los datos leidos del APIDATOS
        '''
        return self.l_datos
    
    def get_l_datos_filtrados(self):
        '''
        Retorna la lista de datos filtrados por dlgid validos
        '''
        return self.l_datos_filtrados

    def get_l_dlgid_validos(self):
        '''
        Retorna la lista l_dlgid
        '''
        return self.l_dlgid

    def read_data_chunk(self):
        '''
        Lee de la API DATOS un paquete de datos (10000). Obtengo una lista de listas, donde estas ultimas
        son: [fechaData, fechaSys, unit_id, medida, valor]
        [
            ['06/23/2023, 04:47:29', '06/23/2023, 04:49:02', 'ARROZUR01', 'q0', 38.46],
            ['06/23/2023, 04:47:29', '06/23/2023, 04:49:02', 'ARROZUR01', 'bt', 0.36],
            ['06/23/2023, 04:48:11', '06/23/2023, 04:49:02', 'SPY003', 'pA', -2.5],
            ['06/23/2023, 04:48:11', '06/23/2023, 04:49:02', 'SPY003', 'pB', -2.5],
            ['06/23/2023, 04:48:11', '06/23/2023, 04:49:02', 'SPY003', 'bt', 12.168]
        ]

        Esta lista actualiza a self.l_datos.
        No retorno nada ya que es muy grande la lista
        '''
        url = f'http://{APIDATOS_HOST}:{APIDATOS_PORT}/apidatos/datos'
        params={'user':APIDATOS_USERKEY}
        try:
            req=requests.get(url=url,params=params,timeout=10)
        except Exception as e:
            print(f"read_data_chunk exception {e}. Exit")
            self.l_datos = []
            return
    
        if req.status_code == 204:
            print('WARN [read_data_chunk] no data!!!')
            self.l_datos = []
            return

        if req.status_code != 200:
            print(f'ERROR [read_data_chunk] status_code != 200 {req.status_code} !!!')
            self.l_datos = []
            return
        #
        # Retorno la lista de datos recibida. Cada datos es una lista
        jd_rsp = req.json()
        self.l_datos = jd_rsp['l_datos']

    def read_dlgid_validos(self):
        """
        Leo de la SQL los dlgid de los equipos válidos y la dejo en l_dlgid
        """
        if not self.bd.connect():
            print ("ERROR [read_dlgid_validos]: No hay conexion a la BD.")
            return

        if self.bd.conn is None:
            print ("ERROR [read_dlgid_validos]: Debe conectase a la BD primero.")
            self.bd.close()
            return
        
        sel  = select( self.tables.tb_equipos.c.dlgid )
        try:
            rp = self.bd.conn.execute( sel )
        except Exception as ex:
            print(f'ERROR [read_dlgid_validos] DATA EXCEPTION: {ex}')
            self.bd.close()
            return

        self.l_dlgid = []
        for t in rp.fetchall():
            self.l_dlgid.append(t[0])

        self.bd.close()

    def filter_lines(self):
        '''
        Recibimos una lista de lista de datos. Filtramos por los dlgid validos.
        Siempre inicializamos l_datos_filtrados
        '''
        self.l_datos_filtrados = []
        if len(self.l_dlgid) > 0:
            for line in self.l_datos:
                if line[2] in self.l_dlgid:
                    self.l_datos_filtrados.append(line)
            #
            return

    def insert_data(self):
        '''
        Esta funcion toma la self.l_datos_filtrados [fechaData, fechaSys, unit_id, medida, valor] con 
        datos a insertar y hace las inserciones en la BD.
        [
            ['06/23/2023, 04:47:29', '06/23/2023, 04:49:02', 'ARROZUR01', 'q0', 38.46],
            ['06/23/2023, 04:47:29', '06/23/2023, 04:49:02', 'ARROZUR01', 'bt', 0.36],
            ['06/23/2023, 04:48:11', '06/23/2023, 04:49:02', 'SPY003', 'pA', -2.5],
            ['06/23/2023, 04:48:11', '06/23/2023, 04:49:02', 'SPY003', 'pB', -2.5],
            ['06/23/2023, 04:48:11', '06/23/2023, 04:49:02', 'SPY003', 'bt', 12.168]
        ]
        '''
        nro_items = len(self.l_datos_filtrados)
        print(f"APIQLIK DATALOADER: ITEMS={nro_items}")

        if not self.bd.connect():
            print ("ERROR [insert_data]: No hay conexion a la BD.")

        if self.bd.conn is None:
            print ("ERROR [insert_data]: Debe conectase a la BD primero.")
            self.bd.close()
            return
        
        insert_stmt = insert(self.tables.tb_datos).values(fechadata = bindparam("py_fechadata"),
                        fechasys = bindparam("py_fechasys"),
                        dlgid = bindparam("dlgid"),
                        tag = bindparam("tag"),
                        valor = bindparam("valor"))
        
        for i, line in enumerate( self.l_datos_filtrados ):
            #print(line)
            fechadata,fechasys,dlgid,tag,valor = line
            #
            try:
                py_fechadata = dt.datetime.strptime(fechadata,'%m/%d/%Y, %H:%M:%S')
                py_fechasys = dt.datetime.strptime(fechasys,'%m/%d/%Y, %H:%M:%S')
            except ValueError as err:
                print(f"ERROR [insert_data] conversion de fecha: {err}")
                continue
            #
            try:
                self.bd.conn.execute(insert_stmt,{'py_fechadata':py_fechadata, 
                                                  'py_fechasys': py_fechasys,
                                                  'dlgid': dlgid,
                                                  'tag': tag,
                                                  'valor': valor } )
            except Exception as ex:
               print(f'ERROR [insert_data] INSERT DATA EXCEPTION: {ex}')
               self.bd.rollback()
               self.bd.close()
               return
            
            if ((i + 1) % 10 == 0 ):
                self.bd.commit()
        #
        self.bd.commit()   
        self.bd.close()

    def run(self):
        """
        Ejecuta el ciclo de corrida:
        """
        _ = self.read_dlgid_validos()
        _ = self.read_data_chunk()
        _ = self.filter_lines()
        if len(self.l_datos_filtrados) > 0:
            _ = self.insert_data()

#------------------------------------------------------------------------------------

def clt_C_handler(signum, frame):
    sys.exit(0)

if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)

    print("ANEP_DATALOADER Starting...")
    print(f'-SLEEP_TIME={SLEEP_TIME}')
    print(f'-APIDATOS={APIDATOS_HOST}/{APIDATOS_PORT}')

    dataloader = Dataloader()

    while True:
        print('Running...')
        
        start_time = time.time()
        dataloader.run()
        elapsed_time = time.time() - start_time
        
        print(f'Elapsed: seconds {elapsed_time}')
        print(f'Sleeping {SLEEP_TIME}...')
        time.sleep(SLEEP_TIME)