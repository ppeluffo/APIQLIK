#!/home/pablo/Spymovil/python/proyectos/APIQLIK/venv/bin/python3

"""
API para proveer datos del prototipo de ANEP a QLICK.
Generamos 2 entry point:
- Uno en el que vamos dando de a 5000 datos en forma consecutiva.
  Debo llevar una marca del ultimo registro que dí.
- Otro que me pide el ID, fecha_ini,fecha_fin y entrego solo esos
  datos si son menos de 5000.

Debemos tener autentificacion BASICA

https://stackoverflow.com/questions/9474397/formatting-sqlalchemy-code


Creacion de tablas desde qtconsole:

from sqlalchemy import select, bindparam, update, Text, and_
from sqlalchemy.dialects.postgresql import insert
import datetime as dt
from base_datos import Bd
import schemas as scm

bd=Bd()
conn = bd.connect()
scm.metadata.create_all(bd.get_engine())

"""

import sys
# Los 2 insert son porque un trabaja para el docker y otro localmente
sys.path.insert(0,'bdatos/')
sys.path.insert(0,'../bdatos/')
import os
import logging
import json
from flask import Flask, request, jsonify,  Response
from flask_restful import Resource, Api, reqparse
from flask_httpauth import HTTPBasicAuth
from sqlalchemy import select, bindparam, update, Text, and_
from sqlalchemy.dialects.postgresql import insert
import datetime as dt
from base_datos import Bd
import schemas as scm

MAX_LINES = int(os.environ.get('MAX_LINES','10000'))

API_VERSION = 'R002 @ 2024-10-22'

app = Flask(__name__)
api = Api(app)
auth = HTTPBasicAuth()

@auth.verify_password
def verify_password(username, password):

    if not (username and password):
        return False
    
    ca = Control_acceso()
    t_user = ca.read_user(username)
    if t_user is None:
        return False
    
    bd_username = t_user[1]
    bd_passwd = t_user[2]

    return bd_passwd == password

class Control_acceso:
    
    def __init__(self):
        self.bd = Bd()
        self.tables = scm

    def read_user(self, username):
        '''
        Lee de la bd:control_acceso los datos del ususario 'username'
        Lee toda la tupla del registro (id,username,passwd,last_row)
        Retorna la primer tupla ( debería ser la unica )o None
        '''
        sel = select(self.tables.tb_clientes).where(self.tables.tb_clientes.c.username == bindparam('username'))
        if not self.bd.connect():
            print(f"ApiQlik ERROR [Control_acceso] no puedo conectarme a la BD.")
            return None
        #
        try:
            rp = self.bd.conn.execute(sel,{'username':username})
        except Exception as ex:
            print(f'ApiQlik ERROR [Control_acceso] DATA EXCEPTION: {ex}')
            self.bd.close()
            return None
        #
        return rp.first()

class Utils:
        
    def __init__(self):
        self.bd = Bd()
        self.tables = scm
        
    def get_last_data_id(self, dlgid):
        """
        Busca si hay algún registro en la BD:tb_control_download con la tupla username_id/equipo_id.
        Si la hay, retorna el valor de last_data_id.
        Si no la hay, tenemos que inicializar creando un registro con el last_data_id = 0 y también
        retornarlo.
        Return:
        None: en caso de error
        Int: id.
        """
        scalar_subq_username = (
            select(self.tables.tb_clientes.c.id).where ( self.tables.tb_clientes.c.username == bindparam("username"))
        ).scalar_subquery()

        scalar_subq_dlgid = (
            select(self.tables.tb_equipos_validos.c.id).where ( self.tables.tb_equipos_validos.c.dlgid == bindparam("dlgid"))
        ).scalar_subquery()

        sel = select(self.tables.tb_control_download.c.last_data_id).where(
                and_(
                    self.tables.tb_control_download.c.cliente_id == scalar_subq_username,
                    self.tables.tb_control_download.c.dlgid_id == scalar_subq_dlgid
                )
            )
        #
        if not self.bd.connect():
            print(f"ApiQlik ERROR [get_last_data_id] no puedo conectarme a la BD.")
            return None
        #
        try:
            rp = self.bd.conn.execute(sel,{'username': auth.current_user(), 'dlgid':dlgid })
        except Exception as ex:
            print(f'ApiQlik ERROR [get_last_data_id] DATA EXCEPTION: {ex}')
            self.bd.close()
            return None           

        res = rp.fetchone()
        #print(f'DEBUG1::get_last_data_id: dlgid={dlgid}, res={res}')
        if res is None:
            """
            No hay registro
            Debo inicializar el registro username,dlgid,row
            """
            print(f"Inicializo control_download para el usuario {auth.current_user()} y el equipo {dlgid}")
            ins  = insert(self.tables.tb_control_download).values(
                    cliente_id = scalar_subq_username,
                    dlgid_id = scalar_subq_dlgid,
                    last_data_id = 0
                    )
            try:
                self.bd.conn.execute(ins,{'username': auth.current_user(), 'dlgid': dlgid} )
            except Exception as ex:
               print(f'ERROR [get_last_data_id] INSERT DATA EXCEPTION: {ex}')
               self.bd.rollback()
               self.bd.close()
               return None
            #
            self.bd.commit()
            self.bd.close()
            return 0
        #
        return res[0]
    
    def update_last_id(self, dlgid, last_data_id):
        '''
        Actualizamos el registro de la tupla username_id/dlgid_id con el valor del 
        la ultima linea entregada (last_data_id)
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [update_last_id] no puedo conectarme a la BD.")
            return None
    
        scalar_subq_username = (
            select(self.tables.tb_clientes.c.id).where ( self.tables.tb_clientes.c.username == bindparam("username"))
        ).scalar_subquery()

        scalar_subq_dlgid = (
            select(self.tables.tb_equipos_validos.c.id).where ( self.tables.tb_equipos_validos.c.dlgid == bindparam("dlgid"))
        ).scalar_subquery()

        upd = update(self.tables.tb_control_download).where(
                and_(
                    self.tables.tb_control_download.c.dlgid_id == scalar_subq_dlgid,
                    self.tables.tb_control_download.c.cliente_id == scalar_subq_username,
                )
            ).values( last_data_id=bindparam("last_data_id") )
            
        try:
            _ = self.bd.conn.execute(upd,{'username':auth.current_user(), 'dlgid':dlgid, 'last_data_id':last_data_id})    
        except Exception as ex:
            print(f'ApiQlik ERROR [update_last_id] DATA EXCEPTION: {ex}')
            self.bd.rollback()
            self.bd.close()
            return False
        
        self.bd.conn.commit()
        self.bd.close()
        return True
    
    def read_dlgids(self):
        '''
        Funcion que propiamente lee la lista de la BD
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_read_dlgids] no puedo conectarme a la BD.")
            return None
        #
        sel  = select( self.tables.tb_equipos_validos.c.dlgid )
        try:
            rp = self.bd.conn.execute( sel )
        except Exception as ex:
            print(f'ApiQlik ERROR [bd_read_dlgids] DATA EXCEPTION: {ex}')
            self.bd.close()
            return None

        l_dlgids = []
        for t in rp.fetchall():
            l_dlgids.append(t[0])

        self.bd.close()
        return l_dlgids
    
    def insert_dlgid(self, dlgid):
        '''
        Funcion que inserta el dlgid en la BD
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_insert_dlgid] no puedo conectarme a la BD.")
            return False
        #
        ins  = insert( self.tables.tb_equipos_validos ).values(dlgid = bindparam('dlgid'))
        try:
            rp = self.bd.conn.execute( ins, { 'dlgid':dlgid } )
        except Exception as ex:
            print(f'ApiQlik ERROR [bd_insert_dlgid] DATA EXCEPTION: {ex}')
            self.bd.close()
            return False

        self.bd.commit()
        self.bd.close()
        return True

    def get_last_data_ids(self, l_dlgids):
        '''
        Toma una lista de dlgids y retorna una lista con tuplas (dlgid/last_data_id)
        '''
        l_last_data_ids = []
        for dlgid in l_dlgids:
            last_data_id = self.get_last_data_id(dlgid)
            if last_data_id is None:
                print(f"ApiQlik ERROR [get_last_data_ids] no puedo inicializar la BD.")
                return None
            else:
                l_last_data_ids.append( (dlgid, last_data_id) )
        return l_last_data_ids

    def read_data(self, l_last_data_ids ):
        '''
        Recibe una lista de tuplas (dlgid/last_data_id) y lee los datos
        Devuelve una lista de tuplas (dlgid, lasta)
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_read_dlgids] no puedo conectarme a la BD.")
            return None
        #
        sel  = select( self.tables.tb_datos ).where(
            and_(
                self.tables.tb_datos.c.id > bindparam('last_data_id'),
                self.tables.tb_datos.c.dlgid == bindparam('dlgid')
            )
        ).limit(MAX_LINES)
        if not self.bd.connect():
            print(f"ApiQlik ERROR [read_data] no puedo conectarme a la BD.")
            return None
        #
        '''
        Armo una lista con tuplas donde c/u tiene el dlgid y el iterable con los resultados
        '''
        l_qres = []
        for t in l_last_data_ids:
            dlgid = t[0]
            last_data_id = t[1]
            try:
                rp = self.bd.conn.execute(sel,{'dlgid':dlgid, 'last_data_id': last_data_id})
            except Exception as ex:
                print(f'ApiQlik ERROR [DownloadDlgid] DATA EXCEPTION: {ex}')
                self.bd.close()
                return None
              
            l_qres.append( (dlgid, rp.fetchall()))
        #
        self.bd.close()
        return l_qres
    
    def update_last_ids(self, d_last_id):
        for dlgid, last_data_id in d_last_id.items():
            self.update_last_id(dlgid, last_data_id)


class Ping(Resource):
    '''
    Prueba la conexion a la SQL
    '''
    @auth.login_required
    def get(self):
        ''' Retorna la versión. Solo a efectos de ver que la api responda
        '''
        return {'rsp':'OK','version':API_VERSION, 'MAX_LINES':MAX_LINES },200

class Dlgids(Resource, Utils):
    '''
    Lee la lista de dlgid validos de la BD y la devuelve
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm

    @auth.login_required
    def get(self):
        '''
        Lee la lista de dlgid validos y la envia en una respuesta
        '''
        l_dlgids = Utils.read_dlgids(self)
        if l_dlgids is None:
            return {'status':'ERR', 'rsp':'ERROR Conexion a BD'}, 404
            
        return {'status':'OK','rsp': l_dlgids }, 200

    @auth.login_required
    def put(self):
        '''
        Recibe el parámetro REQUERIDO dlgid.
        Lee la lista de dlgid validos: si ya está no hace nada y sino lo inserta
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('dlgid',type=str,location='args',required=True)
        args=parser.parse_args()
        #
        if 'dlgid' not in args:
            print(f"ApiQlik ERROR [Dlgids:put]  No dlgid provisto")
            return {'status':'ERR', 'rsp':'ERROR No dlgid'}, 406
        
        dlgid = args.get('dlgid','')

        # Leemos la lista para ver que ya no exista
        l_dlgids = Utils.read_dlgids(self)
        if l_dlgids is None:
            return {'status':'ERR', 'rsp':'ERROR access BD.'}, 406
        
        if dlgid in l_dlgids:
            # Ya existe
            return {'status':'OK' }, 200

        # Lo inserto. No controlo errores
        _ = Utils.insert_dlgid(self, dlgid)
        return {'status':'OK' }, 200

class Help(Resource):
    '''
    Clase informativa
    '''
    @auth.login_required
    def get(self):
        ''' Retorna la descripcion de los metodos disponibles
        '''
        d_options = {
            'GET /apiqlik/ping':'Prueba la respuesta',
            'GET /apiqlik/help':'Esta pantalla de ayuda',
            'GET /apiqlik/dlgids':'Devuelve la lista de dlgids validos',
            'PUT /apiqlik/dlgids':'Agrega un nuevo dlgid al sistema',
            'GET /apiqlik/download_data':'Devuelve todos los datos',
            'GET /apiqlik/download_dlgid':'Permite seleccionar dlgid',
            'GET /apiqlik/download_dlgid_list':'Baja los datos de una lista de dlgids',
        }
        return d_options, 200

class Download(Resource):
    ''' 
    Devuelve hasta 5000 registros en modo csv.
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm
        
    @auth.login_required
    def get(self):
        '''
        Cada vez que me consultan, actualizo el registro 'tables.tb_control_acceso.last_row
        Este indica para c/usuario que consulta, cual fue la ultima linea que se le dió y sirve
        para enviar de esta en adelante.
        '''
        return {'status':'ERR', 'code': 204, 'rsp':'DEPRECATED'}
    
        scalar_subq_lastrow = ( 
            select(self.tables.tb_control_acceso.c.last_row).where(
                self.tables.tb_control_acceso.c.username == bindparam("username")
                ).scalar_subquery()
            )
        
        sel  = select( self.tables.tb_datos ).where(self.tables.tb_datos.c.id > scalar_subq_lastrow).limit(MAX_LINES)
        
        if not self.bd.connect():
            print(f"ApiQlik ERROR [Download] no puedo conectarme a la BD.")
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR Conexion a BD'}
        #
        try:
            rp = self.bd.conn.execute(sel,{'username': auth.current_user()})
        except Exception as ex:
            print(f'ApiQlik ERROR [Download] DATA EXCEPTION: {ex}')
            self.bd.close()
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR BD select'}               
        #
               # Las consultas siempre devuelven un result_proxy
        nro_lines = 0
        csv_data = ""
        for rcd in rp.fetchall():
            (id,fechasys,fechadata,dlgid,tag,value) = rcd
            line = f'{id},{fechasys},{fechadata},{dlgid},{tag},{value}\n'
            csv_data += line
            nro_lines += 1
            last_row = id
        #
        #print(f"DEBUG: Last Row={last_row}")
        #
        # Actualizo el ultimo registro en Control_acceso.
        if nro_lines > 0:
            #print(f"DEBUG nro_lines={nro_lines}")
            upd = update(self.tables.tb_control_acceso).where(
                    self.tables.tb_control_acceso.c.username == bindparam("name")).values(
                    last_row=bindparam("last_row")
                    )
            try:
                rp = self.bd.conn.execute(upd,{'name':auth.current_user(), 'last_row':last_row})    
            except Exception as ex:
                print(f'ApiQlik ERROR [Download] DATA EXCEPTION: {ex}')
                self.bd.close()
                return {'status':'ERR', 'code': 404, 'rsp':'ERROR BD select'}
            self.bd.conn.commit()
        #
        self.bd.close()
        response = Response(csv_data, content_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=datos.csv"
        return response
 
class Read(Resource):
    ''' 
    Devuelve registros en modo csv sin marcarlos.
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm
        
    @auth.login_required
    def get(self):
        '''
        Leo los datos entre las 2 fechas: fechaini, fechafin y retorno todos
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('fechaini',type=str,location='args',required=True)
        parser.add_argument('fechafin',type=str,location='args',required=True)
        args=parser.parse_args()
        fechaini = args['fechaini']
        fechafin = args['fechafin']

        sel  = select( self.tables.tb_datos ).where(
            and_(
                self.tables.tb_datos.c.fechadata >= bindparam('fechaini'),
                self.tables.tb_datos.c.fechadata <= bindparam('fechafin')
            )
        )
        
        if not self.bd.connect():
            print(f"ApiQlik ERROR [Download] no puedo conectarme a la BD.")
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR Conexion a BD'}
        #
        try:
            rp = self.bd.conn.execute(sel,{'fechaini':fechaini, 'fechafin':fechafin})
        except Exception as ex:
            print(f'ApiQlik ERROR [Download] DATA EXCEPTION: {ex}')
            self.bd.close()
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR BD select'}               
        #
        # Las consultas siempre devuelven un result_proxy
        print(f"ApiQlik Read from {fechaini} to {fechafin}")

        nro_lines = 0
        csv_data = ""
        for rcd in rp.fetchall():
            (id,fechasys,fechadata,dlgid,tag,value) = rcd
            line = f'{id},{fechasys},{fechadata},{dlgid},{tag},{value}\n'
            csv_data += line
            nro_lines += 1
        #
        self.bd.close()
        response = Response(csv_data, content_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=datos.csv"
        return response
 
class DownloadDlgid(Resource, Utils):
    ''' 
    Devuelve hasta 5000 registros en modo csv de un dataloggers dado
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm
    
    @auth.login_required
    def get(self):
        '''
        Cada vez que me consultan, actualizo el registro 'tables.tb_control_acceso.last_row
        Este indica para c/usuario que consulta, cual fue la ultima linea que se le dió y sirve
        para enviar de esta en adelante.
        Solo se dan los datos de un dlgid dado
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('dlgid',type=str,location='args',required=True)
        args=parser.parse_args()
        dlgid = args['dlgid']
        #print(f'DEBUG::DownloadDlgid dlgid={dlgid}')
        '''
        Buscamos / creamos registro con ultimo id
        '''
        last_data_id = Utils.get_last_data_id(self, dlgid)
        #last_data_id = self.get_last_data_id(dlgid)
        if last_data_id is None:
            print(f"ApiQlik ERROR [DownloadDlgid] no puedo inicializar la BD.")
            return {'status':'ERR','rsp':'ERROR Inicializar BD'}, 404
        #           
        #print(f'DEBUG::DownloadDlgid last_data_id={last_data_id}')

        sel  = select( self.tables.tb_datos ).where(
            and_(
                self.tables.tb_datos.c.id > bindparam('last_data_id'),
                self.tables.tb_datos.c.dlgid == bindparam('dlgid')
            )
        ).limit(MAX_LINES)
        
        if not self.bd.connect():
            print(f"ApiQlik ERROR [DownloadDlgid] no puedo conectarme a la BD.")
            return {'status':'ERR','rsp':'ERROR Conexion a BD'}, 404
        #
        try:
            rp = self.bd.conn.execute(sel,{'dlgid':dlgid, 'last_data_id': last_data_id})
        except Exception as ex:
            print(f'ApiQlik ERROR [DownloadDlgid] DATA EXCEPTION: {ex}')
            self.bd.close()
            return {'status':'ERR', 'rsp':'ERROR BD select'}, 404            
        #
        # Las consultas siempre devuelven un result_proxy
        nro_lines = 0
        csv_data = ""
        for rcd in rp.fetchall():
            (id,fechasys,fechadata,dlgid,tag,value) = rcd
            line = f'{id},{fechasys},{fechadata},{dlgid},{tag},{value}\n'
            csv_data += line
            nro_lines += 1
            last_data_id = id
        #
        #print(f"DEBUG::DownloadDlgid Last Row=last_data_id}")
        #
        # Actualizo el ultimo registro en Control_acceso.
        if nro_lines > 0:
            #self.update_last_id(dlgid, last_data_id) 
            Utils.update_last_id(self, dlgid, last_data_id) 
        #
        self.bd.close()
        response = Response(csv_data, content_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=datos.csv"
        return response

class DownloadDlgidList(Resource, Utils):
    ''' 
    Devuelve hasta 5000 registros en modo csv de todos los dataloggers de una lista
    que recibo por POST
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm

    @auth.login_required
    def post(self):
        '''
        Recibo un JSON con una lista de dataloggers.
        '''
        # Extraigo los datos del json ( es un diccionario serializado json)
        params = request.get_json()
        l_dlgids = params.get('l_dlgid',[])
        '''
        Paso 1: Selecciono los last_data_id para c/dlgid de l_dlgid.
                Dejo en una lista tuplas (dlgid, last_data_id)
        '''
        l_last_data_ids = Utils.get_last_data_ids(self, l_dlgids)
        if l_last_data_ids is None:
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR get_last_data_ids'}
    
        #print(f'DEBUG::DownloadDlgidList l_last_data_ids = {l_last_data_ids}' )
        '''
        Paso 2: Leo los datos de todos los dlgid de la l_dlgid y devulevo una lista 
        con tuplas de 3 elementos: dlgid/last_data_id/data_iterable
        '''
        l_qres = Utils.read_data(self, l_last_data_ids)
        if l_qres is None:
            return {'status':'ERR','rsp':'ERROR read_data'}, 404
        
        #print(f'DEBUG::DownloadDlgidList l_qres = {l_qres}' )
        '''
        Genero el csv con los datos de todos los dlgids
        Cada elemento de la l_qres es una tupla que tiene (dlgid, [lista de lineas de datos])
        '''
        l_zip_data = []
        for (id, l_data) in l_qres:
            l_zip_data.append(l_data)

        #print(f'DEBUG::DownloadDlgidList l_zip_data = {l_zip_data}')

        # uso el * para unpack ya que no tengo la cantidad de listas
        csv_data = ""
        d_last_id = {}
        nro_lines = 0
        for tdata in zip(*l_zip_data):
            for rcd in tdata:
                (id,fechasys,fechadata,dlgid,tag,value) = rcd
                line = f'{id},{fechasys},{fechadata},{dlgid},{tag},{value}\n'
                csv_data += line
                d_last_id[dlgid] = int(id)
                nro_lines += 1
            if nro_lines > MAX_LINES:
                break

        #print(f'DEBUG::DownloadDlgidList csv_data = {csv_data}')
        #print(f'DEBUG::DownloadDlgidList d_last_id = {d_last_id}')
        '''
        Paso 3: Actualizo la tabla de control_download
        '''
        _ = Utils.update_last_ids(self, d_last_id)

        # Retorno los datos
        self.bd.close()
        response = Response(csv_data, content_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=datos.csv"
        return response

class Housekeeping(Resource):
    ''' 
    Clase oculta para realizar tareas de mantenimiento
    La tabla clientes no la puedo crear !!! ya que la uso para autentificarme
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm
        
    @auth.login_required
    def get(self):
        '''
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('action',type=str,location='args',required=True)
        args=parser.parse_args()
        action = args['action']

        if action == 'CREATE_DATABASES':
            conn = self.bd.connect()
            self.tables.metadata.create_all(self.bd.get_engine())
            return {'status':'OK', 'rsp':'CREATE_DATABASES'}, 200   

        return {'status':'OK', 'rsp':'NO ACTION'}, 200  

     
api.add_resource( Ping, '/apiqlik/ping')
api.add_resource( Help, '/apiqlik/help')
api.add_resource( Dlgids, '/apiqlik/dlgids')
api.add_resource( Download, '/apiqlik/download')
api.add_resource( DownloadDlgid, '/apiqlik/download_dlgid')
api.add_resource( DownloadDlgidList, '/apiqlik/download_dlgid_list')
api.add_resource( Read, '/apiqlik/read')
#api.add_resource( Housekeeping, '/apiqlik/housekeeping')

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info( f'Starting APIQLIK, MAX_LINES={MAX_LINES}' )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5022, debug=True)