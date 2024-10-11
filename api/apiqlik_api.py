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

"""

import sys
# Los 2 insert son porque un trabaja para el docker y otro localmente
sys.path.insert(0,'bdatos/')
sys.path.insert(0,'../bdatos/')
import os
import logging
from flask import Flask, request, jsonify,  Response
from flask_restful import Resource, Api, reqparse
from flask_httpauth import HTTPBasicAuth
from sqlalchemy import select, bindparam, update, Text
from sqlalchemy.dialects.postgresql import insert
import datetime as dt
from base_datos import Bd
import schemas as scm

MAX_LINES = os.environ.get('MAX_LINES','4')

API_VERSION = 'R001 @ 2024-10-09'

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
        #print('Control Acceso')

    def read_user(self, username):
        '''
        Lee de la bd:control_acceso los datos del ususario 'username'
        Lee toda la tupla del registro (id,username,passwd,last_row)
        Retorna la primer tupla ( debería ser la unica )o None
        '''
        sel = select(self.tables.tb_control_acceso).where(self.tables.tb_control_acceso.c.username == bindparam('username'))
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
            
class Ping(Resource):
    '''
    Prueba la conexion a la SQL
    '''
    @auth.login_required
    def get(self):
        ''' Retorna la versión. Solo a efectos de ver que la api responda
        '''
        return {'rsp':'OK','version':API_VERSION },200

class Dlgids(Resource):
    '''
    Lee la lista de dlgid validos de la BD y la devuelve
    '''
    def __init__(self):
        self.bd = Bd()
        self.tables = scm

    def bd_read_dlgids(self):
        '''
        Funcion que propiamente lee la lista de la BD
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_read_dlgids] no puedo conectarme a la BD.")
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR Conexion a BD'}
        #
        sel  = select( self.tables.tb_equipos_validos.c.dlgid )
        try:
            rp = self.bd.conn.execute( sel )
        except Exception as ex:
            print(f'ApiQlik ERROR [bd_read_dlgids] DATA EXCEPTION: {ex}')
            self.bd.close()
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR BD select'}

        l_dlgid = []
        for t in rp.fetchall():
            l_dlgid.append(t[0])

        self.bd.close()
        return {'status':'OK', 'code': 200, 'rsp': l_dlgid }
    
    def bd_insert_dlgid(self, dlgid):
        '''
        Funcion que inserta el dlgid en la BD
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_insert_dlgid] no puedo conectarme a la BD.")
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR Conexion a BD'}
        #
        ins  = insert( self.tables.tb_equipos_validos ).values(dlgid = bindparam('dlgid'))
        try:
            rp = self.bd.conn.execute( ins, { 'dlgid':dlgid } )
        except Exception as ex:
            print(f'ApiQlik ERROR [bd_insert_dlgid] DATA EXCEPTION: {ex}')
            self.bd.close()
            return {'status':'ERR', 'code': 404, 'rsp':'ERROR BD insert'}

        self.bd.commit()
        self.bd.close()
        return {'status':'OK', 'code': 200, 'rsp':'OK' }

    @auth.login_required
    def get(self):
        '''
        Lee la lista de dlgid validos y la envia en una respuesta
        '''
        d_rsp = self.bd_read_dlgids()
        status = d_rsp['status']
        code = d_rsp['code']
        rsp = d_rsp['rsp']
        return {'rsp':rsp}, code

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
            d_rsp = {'rsp':'ERROR', 'msg':'ERROR No dlgid'}
            return d_rsp, 406
        
        dlgid = args.get('dlgid','')

        # Leemos la lista para ver que ya no exista
        d_rsp = self.bd_read_dlgids()
        status = d_rsp['status']
        code = d_rsp['code']
        rsp = d_rsp['rsp']

        if status == 'OK':
            l_dlgids = rsp
            if dlgid in l_dlgids:
                # Ya existe
                return {'rsp':'OK'},200

        # Lo inserto
        d_rsp = self.bd_insert_dlgid(dlgid)
        status = d_rsp['status']
        code = d_rsp['code']
        rsp = d_rsp['rsp']
        return {'rsp':rsp}, code

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
            'GET /apiqlik/filter':'Permite seleccionar dlgid,start,end',
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
 
api.add_resource( Ping, '/apiqlik/ping')
api.add_resource( Help, '/apiqlik/help')
api.add_resource( Dlgids, '/apiqlik/dlgids')
api.add_resource( Download, '/apiqlik/download')

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info( f'Starting APIQLIK' )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5022, debug=True)