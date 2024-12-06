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

VERSION 2.0 @ 2024-11-28
Las tablas de dataloggers deben ser por usuario. Cada usuario va a tener su propia
lista de equipos en los que puede trabajar.
Esto es porquie sino el primer usuario que se conecta, ya baja los datos de la lista
y luego otros usuarios no los ven
Tenemos una tabla de equipos, una de usuarios y una de nodos: estos son una relación entre
un usuario y un equipo.
-Entrypoint dlgid:
 GET: Lee la lista de nodos, aquellos que pertenecen al usuario y devuelve sus dlgid
 PUT: Inserta el dlgid en la tabla de equipos y luego crea el nodo

- En la tabla 'tb_equipos_validos' agregamos una referencia 'cliente_id'
- Utils:read_dlgids, Utils:insert_dlgid usan el user_id.

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
from sqlalchemy import select, delete, bindparam, update, Text, and_
from sqlalchemy.dialects.postgresql import insert
import datetime as dt
from base_datos import Bd
import schemas as scm

MAX_LINES = int(os.environ.get('MAX_LINES','10000'))
#MAX_LINES = int(os.environ.get('MAX_LINES','10'))
HOUSEKEEPING = os.environ.get('HOUSEKEEPING','TRUE')

API_VERSION = 'R002 @ 2024-12-05'

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
        Lee de la bd:tb_clientes los datos del ususario 'username'
        Lee toda la tupla del registro (id,username,passwd,last_row)
        Retorna la primer tupla ( debería ser la unica )o None
        '''
        sel = select(self.tables.tb_usuarios).where(self.tables.tb_usuarios.c.username == bindparam('username'))
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
 
    def create_dlgid(self, dlgid):
        '''
        Crea una entrada nueva en la tabla de equipos.
        Si existe el dlgid, no hace nada
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [create_dlgid] no puedo conectarme a la BD.")
            return False
        
        ins  = insert( self.tables.tb_equipos ).values(dlgid = bindparam('dlgid')).on_conflict_do_nothing()
        try:
            res = self.bd.conn.execute( ins, { 'dlgid':dlgid } )
        except Exception as ex:
            print(f'ApiQlik ERROR [create_dlgid] DATA EXCEPTION: {ex}')
            self.bd.close()
            return False

        self.bd.commit()
        self.bd.close()
        try:
            pk = res.inserted_primary_key[0]
        except:
            # No hizo nada la consulta, posiblemente por 'on_conflict'
            pk = None
        
        return pk  

    def create_nodo(self, dlgid):
        '''
        Creamos un nodo entre el dlgid y el usuario.
        Si ya existe ignoramos el error.
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [create_nodo] no puedo conectarme a la BD.")
            return False
        
        # Subquery para obtener el user_id del username.
        scalar_subq_user_id = (
            select(self.tables.tb_usuarios.c.id).where ( self.tables.tb_usuarios.c.username == bindparam("username"))
        ).scalar_subquery()
        #
        # Subquery para obtener el equipo_id del dlg.
        scalar_subq_equipo_id = (
            select(self.tables.tb_equipos.c.id).where ( self.tables.tb_equipos.c.dlgid == bindparam("dlgid"))
        ).scalar_subquery()
        #
        ins  = insert( self.tables.tb_nodos ).values( equipo_id = scalar_subq_equipo_id, user_id = scalar_subq_user_id ).on_conflict_do_nothing()
        try:
            res = self.bd.conn.execute( ins, { 'dlgid':dlgid, 'username':auth.current_user()} )
        except Exception as ex:
            print(f'ApiQlik ERROR [create_nodo] DATA EXCEPTION: {ex}')
            self.bd.close()
            return False

        self.bd.commit()
        self.bd.close()
        try:
            pk = res.inserted_primary_key[0]
        except:
            # No hizo nada la consulta, posiblemente por 'on_conflict'
            pk = None
        
        return pk   

    def create_control_download_entry(self, nodo_id):
        '''
        Creo una nueva entrada en la tabla control_download, con el valor
        de last_data_id inicializado en 0.
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [create_control_download_entry] no puedo conectarme a la BD.")
            return False
        #
        ins  = insert( self.tables.tb_control_download ).values(nodo_id = bindparam('nodo_id')).on_conflict_do_nothing()
        try:
            res = self.bd.conn.execute( ins, { 'nodo_id':nodo_id } )
        except Exception as ex:
            print(f'ApiQlik ERROR [create_control_download_entry] DATA EXCEPTION: {ex}')
            self.bd.close()
            return False

        self.bd.commit()
        self.bd.close()
        try:
            pk = res.inserted_primary_key[0]
        except:
            # No hizo nada la consulta, posiblemente por 'on_conflict'
            pk = None
        
        return pk  

    def read_nodos(self):
        '''
        Lee todos los nodos que tiene un usuario dado y genera una lista
        de tuplas (nodo_id, dlg_id, dlgid) que retorna

        SELECT (nodos.id, equipo_id, dlgid) 
        FROM nodos, equipos 
        WHERE nodos.equipo_id = equipos.id 
        AND user_id = ( SELECT (id) FROM usuarios WHERE username = '?')

        print(sel)
        SELECT nodos.id, nodos.equipo_id, equipos.dlgid 
        FROM nodos, equipos 
        WHERE nodos.equipo_id = equipos.id 
        AND nodos.user_id = (SELECT usuarios.id FROM usuarios WHERE usuarios.username = :username)
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [read_nodos] no puedo conectarme a la BD.")
            return False

        # Subquery para obtener el user_id del username.
        scalar_subq_user_id = (
            select(self.tables.tb_usuarios.c.id).where ( self.tables.tb_usuarios.c.username == bindparam("username"))
        ).scalar_subquery()
        #
        sel = select(self.tables.tb_nodos.c.id, self.tables.tb_nodos.c.equipo_id, self.tables.tb_equipos.c.dlgid).where(
                and_(
                    self.tables.tb_nodos.c.equipo_id == self.tables.tb_equipos.c.id, 
                    self.tables.tb_nodos.c.user_id == scalar_subq_user_id
                )
            )
        # Armo una lista con tuplas donde c/u tiene el dlgid y el iterable con los resultados
        try:
            rp = self.bd.conn.execute(sel,{'username':auth.current_user() })
        except Exception as ex:
            print(f'ApiQlik ERROR [read_nodos] DATA EXCEPTION: {ex}')
            self.bd.close()
            return None
        #
        res = rp.fetchall()
        self.bd.close()
        return res

    def read_data(self, dlgid, maxlines=MAX_LINES):
        '''
        Lee los datos desde el last_data_id
        hasta el final o maxlines de un nodo dado (dlgid, userid)
        Devuelve una tupla con una lista con los datos
        Actualizo en control_download el last_data_id para el nodo dado
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [read_data] no puedo conectarme a la BD.")
            return None
        #
        # Subquery para obtener el user_id del username.
        scalar_subq_user_id = (
            select(self.tables.tb_usuarios.c.id).where ( self.tables.tb_usuarios.c.username == bindparam("username"))
        ).scalar_subquery()
        #        
        # Subquery para obtener el equipo_id del dlg.
        scalar_subq_equipo_id = (
            select(self.tables.tb_equipos.c.id).where ( self.tables.tb_equipos.c.dlgid == bindparam("dlgid_1"))
        ).scalar_subquery()
        #
        # Subquery para obtener el nodo_id para el (user_id, equipo_id).
        scalar_subq_nodo_id = (
            select(self.tables.tb_nodos.c.id).where(
                and_(
                    self.tables.tb_nodos.c.equipo_id == scalar_subq_equipo_id, 
                    self.tables.tb_nodos.c.user_id == scalar_subq_user_id
                )
            )
        ).scalar_subquery()
        #
        # Subquery para obtener el last_data_id para el nodo (username, dlgid).
        scalar_subq_last_data_id = (
            select(self.tables.tb_control_download.c.last_data_id).where ( self.tables.tb_control_download.c.nodo_id == scalar_subq_nodo_id )
        ).scalar_subquery()
        #
        # Query para leer los datos de un datalogger dado de un nodo, a partir del last_data_id del nodo
        sel  = select( self.tables.tb_datos ).where(
            and_(
                self.tables.tb_datos.c.id > scalar_subq_last_data_id,
                self.tables.tb_datos.c.dlgid == bindparam('dlgid_2')
            )
        ).limit(maxlines)
        #
        '''
        Armo una lista con tuplas donde c/u tiene el dlgid y el iterable con los resultados
        '''
        try:
            rp = self.bd.conn.execute(sel,{'username':auth.current_user(), 'dlgid_1':dlgid, 'dlgid_2':dlgid})
        except Exception as ex:
            print(f'ApiQlik ERROR [read_data] DATA EXCEPTION: {ex}')
            self.bd.close()
            return None
        #
        data_lines = rp.fetchall()
        self.bd.close()
        return data_lines

    def update_last_id(self, dlgid, last_data_id):
        '''
        Actualizamos el registro del control_download que corresponde al nodo dado
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [update_last_id] no puedo conectarme a la BD.")
            return None
        
        # Subquery para obtener el user_id del username.
        scalar_subq_user_id = (
            select(self.tables.tb_usuarios.c.id).where ( self.tables.tb_usuarios.c.username == bindparam("username"))
        ).scalar_subquery()
        #        
        # Subquery para obtener el equipo_id del dlg.
        scalar_subq_equipo_id = (
            select(self.tables.tb_equipos.c.id).where ( self.tables.tb_equipos.c.dlgid == bindparam("dlgid"))
        ).scalar_subquery()
        #
        # Subquery para obtener el nodo_id para el (user_id, equipo_id).
        scalar_subq_nodo_id = (
            select(self.tables.tb_nodos.c.id).where(
                and_(
                    self.tables.tb_nodos.c.equipo_id == scalar_subq_equipo_id, 
                    self.tables.tb_nodos.c.user_id == scalar_subq_user_id
                )
            )
        ).scalar_subquery()
        #
        # Update query para actualizar el last_data_id
        upd = update(self.tables.tb_control_download).where( self.tables.tb_control_download.c.nodo_id == scalar_subq_nodo_id
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

class Ping(Resource):
    '''
    Prueba la conexion a la SQL
    '''
    @auth.login_required
    def get(self):
        ''' Retorna la versión. Solo a efectos de ver que la api responda
        '''
        return {'rsp':'OK','version':API_VERSION, 'MAX_LINES':MAX_LINES },200

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
            'POST /apiqlik/download_dlgid_list':'Baja los datos de una lista de dlgids',
            'POST /apiqlik/rollback':'Borra los datos de una lista de dlgids',
        }
        return d_options, 200

class Dlgids(Resource, Utils):
    '''
    Lee la lista de dlgid validos de la BD y la devuelve
    '''
    def __init__(self):
        Utils.__init__(self)

    @auth.login_required
    def get(self):
        '''
        Lee la lista de dlgid validos y la envia en una respuesta
        '''
        # Tupla de nodos ( nodo_id, dlg_id, dlgid )
        l_nodos = self.read_nodos()
        l_dlgids = [ dlgid for _, _, dlgid in l_nodos ]
            
        return {'status':'OK','rsp': l_dlgids }, 200

    @auth.login_required
    def put(self):
        '''
        Recibe el dlgid como parámetro REQUERIDO.
        Trata de insertarlo en la tabla de 'equipos'
        Trata de crear el nodo en la tabla 'nodos'
        Si en alguno ya existe ignoramos el error.
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('dlgid',type=str,location='args',required=True)
        args=parser.parse_args()
        #
        if 'dlgid' not in args:
            print(f"ApiQlik ERROR [Dlgids:put]  No dlgid provisto")
            return {'status':'ERR', 'rsp':'ERROR No dlgid'}, 406
        
        dlgid = args.get('dlgid','')

        # Paso 1: Creamos el dlgid en la tabla de equipos
        _ = self.create_dlgid(dlgid)

        # Paso 2: Creamos el nodo
        nodo_id = self.create_nodo(dlgid)

        # Paso 3: Creo una nueva entrada en control_download
        if nodo_id is not None:
            _ = self.create_control_download_entry(nodo_id)

        return {'status':'OK' }, 200

class DownloadDlgid(Resource, Utils):
    ''' 
    Devuelve hasta 5000 registros en modo csv de un dataloggers dado
    '''
    def __init__(self):
        Utils.__init__(self)
    
    @auth.login_required
    def get(self):
        '''
        Cada vez que me consultan, actualizo el registro 'tables.tb_control_acceso.last_row
        Este indica para c/usuario que consulta, cual fue la ultima linea que se le dió y sirve
        para enviar de esta en adelante.
        Solo se dan los datos de un dlgid dado
        SELECT FROM tb_datos WHERE tb_datos.dlgid = ?
        AND tb_datos.id > ( SELECT last_data_id FROM tb_control_download WHERE nodo_id = 

        
        Selecciono los datos para el nodo dado a partir del ultimo last_data_id.
        Actualizo para el nodo dado el last_data_id.
    
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('dlgid',type=str,location='args',required=True)
        args=parser.parse_args()
        dlgid = args['dlgid']
        '''
        Buscamos / creamos registro con ultimo id para el usuario dado
        '''
        l_data = self.read_data(dlgid)
        nro_lines = 0
        last_data_id = 0
        csv_data = ""
        for rcd in l_data:
            (id,fechasys,fechadata,dlgid,tag,value) = rcd
            line = f'{id},{fechasys},{fechadata},{dlgid},{tag},{value}\n'
            csv_data += line
            nro_lines += 1
            last_data_id = id
        #
        #print(f"DEBUG::DownloadDlgid Last Row={last_data_id}")
        #
        # Actualizo el ultimo registro en Control_acceso para el dlgid y el usuario dado
        if nro_lines > 0:
            #self.update_last_id(dlgid, last_data_id) 
            self.update_last_id(dlgid, last_data_id) 
        #
        response = Response(csv_data, content_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=datos.csv"
        return response

class Rollback(Resource, Utils):
    ''' 
    Pone el last_data_id del nodo correspondiente en 0
    Recibe una lista de dlgids.
    '''
    def __init__(self):
        Utils.__init__(self)

    @auth.login_required
    def post(self):
        '''
        Recibo un JSON con una lista de dataloggers.
        Para c/u, determino su nodo y pongo el last_data_id de la tabla control_download en 0.
        '''
        # Extraigo los datos del json ( es un diccionario serializado json)
        params = request.get_json()
        l_dlgids = params.get('l_dlgid',[])
        
        for dlgid in l_dlgids:
            self.update_last_id(dlgid, 0)

        
        return {'status':'OK'}, 200 

class DownloadDlgidList(Resource, Utils):
    ''' 
    Devuelve hasta 5000 registros en modo csv de todos los dataloggers de una lista
    que recibo por POST
    '''
    def __init__(self):
        Utils.__init__(self)

    @auth.login_required
    def post(self):
        '''
        Recibo un JSON el nro.de lineas que quiero en el CSV y una lista de dataloggers
        Voy leyendo los nodos correspondientes y armando un csv hasta alcanzar el maxlines
        y lo transmito.
        '''
        # Extraigo los datos del json ( es un diccionario serializado json)
        params = request.get_json()
        maxlines = int(params.get('maxlines',MAX_LINES))
        l_dlgids = params.get('l_dlgid',[])

        csv_data = ""
        nro_lines_in_csv = 0
        
        for (dlgid) in l_dlgids:
            read_lines = maxlines - nro_lines_in_csv
            if read_lines > 0:
                # Si queda espacio, sigo leyendo
                l_data = self.read_data(dlgid, read_lines)
                last_data_id = 0
                for rcd in l_data:
                    (id,fechasys,fechadata,dlgid,tag,value) = rcd
                    line = f'{id},{fechasys},{fechadata},{dlgid},{tag},{value}\n'
                    csv_data += line
                    nro_lines_in_csv += 1
                    last_data_id = id
                #
                # Actualizo el ultimo registro en Control_acceso para el dlgid y el usuario dado
                self.update_last_id(dlgid, last_data_id) 
            else:
                # No queda mas espacio en el csv: salgo
                break

        # Retorno los datos
        response = Response(csv_data, content_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=datos.csv"
        return response

class Housekeeping(Resource, Utils):
    ''' 
    Clase oculta para realizar tareas de mantenimiento
    La tabla clientes no la puedo crear !!! ya que la uso para autentificarme
    '''
    def __init__(self):
        Utils.__init__(self)
        
    @auth.login_required
    def post(self):
        '''
        Creamos la lista de dlgids, nodos
        '''
        if HOUSEKEEPING == 'FALSE':
            return {'status':'FAIL' }, 405 
           
        params = request.get_json()
        l_dlgids = params.get('l_dlgid',[])
        #
        for dlgid in l_dlgids:
            # Paso 1: Creamos el dlgid en la tabla de equipos
            _ = self.create_dlgid(dlgid)

            # Paso 2: Creamos el nodo
            nodo_id = self.create_nodo(dlgid)

            # Paso 3: Creo una nueva entrada en control_download
            if nodo_id is not None:
                _ = self.create_control_download_entry(nodo_id)

        return {'status':'OK' }, 200


# DEPRECATED
"""
   def get_last_data_id(self, dlgid):
        '''
        Busca si hay algún registro en la BD:tb_control_download con la tupla username_id/equipo_id.
        Si la hay, retorna el valor de last_data_id.
        Si no la hay, tenemos que inicializar creando un registro con el last_data_id = 0 y también
        retornarlo.
        Return:
        None: en caso de error
        Int: id.
        '''

        #print(f"DEBUG: dlgid={dlgid}, username={auth.current_user()}")
              
        scalar_subq_cliente_id = (
            select(self.tables.tb_clientes.c.id).where ( self.tables.tb_clientes.c.username == bindparam("username"))
        ).scalar_subquery()

        scalar_subq_dlgid = (
            select(self.tables.tb_equipos_validos.c.id).where ( 
                and_(
                    self.tables.tb_equipos_validos.c.dlgid == bindparam("dlgid"),
                    self.tables.tb_equipos_validos.c.cliente_id == scalar_subq_cliente_id
                )
            )
        ).scalar_subquery()

        sel = select(self.tables.tb_control_download.c.last_data_id).where(
                and_(
                    self.tables.tb_control_download.c.cliente_id == scalar_subq_cliente_id,
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
            '''
            No hay registro
            Debo inicializar el registro username,dlgid,row
            '''
            print(f"El equipo {dlgid} no esta inicializado o no pertence al usuario {auth.current_user()}")
            self.bd.close()
            return None
        #
        return res[0]
       
    def read_dlgids(self):
        '''
        Funcion que propiamente lee la lista de la BD perteneciente al usuario logueado
        Modificacion 2024-11-28: 
        En la consulta usamos el username de modo que las listas de dlgid son de
        cada usuario
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_read_dlgids] no puedo conectarme a la BD.")
            return None
        #
        scalar_subq_cliente_id = (
            select(self.tables.tb_clientes.c.id).where ( self.tables.tb_clientes.c.username == bindparam("username"))
        ).scalar_subquery()
        #
        sel  = select( self.tables.tb_equipos_validos.c.dlgid ).where( 
            self.tables.tb_equipos_validos.c.cliente_id == scalar_subq_cliente_id 
            )
        #
        try:
            rp = self.bd.conn.execute( sel, {'username':auth.current_user()})    
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
        Modificacion 2024-11-28:
        Agregamos como parámetro el cliente_id de modo que las listas sean pesonificadas.
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [bd_insert_dlgid] no puedo conectarme a la BD.")
            return False
        #
        scalar_subq_cliente_id = (
            select(self.tables.tb_clientes.c.id).where ( self.tables.tb_clientes.c.username == bindparam("username"))
        ).scalar_subquery()
            
        ins  = insert( self.tables.tb_equipos_validos ).values(dlgid = bindparam('dlgid'), cliente_id = scalar_subq_cliente_id )
        try:
            rp = self.bd.conn.execute( ins, { 'dlgid':dlgid, 'username':auth.current_user() } )
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
  
    def update_last_ids(self, d_last_id):
        for dlgid, last_data_id in d_last_id.items():
            self.update_last_id(dlgid, last_data_id)

    def reset_control_download(self, l_dlgids):
        '''
        Recibe una lista de dlgid_id y para el usuario dado, borra las 
        entradas de la tabla tb_control_download
        '''
        if not self.bd.connect():
            print(f"ApiQlik ERROR [reset_control_download] no puedo conectarme a la BD.")
            return None
        #
        scalar_subq_user_id = (
            select(self.tables.tb_clientes.c.id).where ( self.tables.tb_clientes.c.username == bindparam("username"))
        ).scalar_subquery()
        
        scalar_subq_dlgid_id = (
            select(self.tables.tb_equipos_validos.c.id).where ( self.tables.tb_equipos_validos.c.dlgid == bindparam("dlgid"))
        ).scalar_subquery()    
        
        delstmt = delete( self.tables.tb_control_download).where(
            and_(
                self.tables.tb_control_download.c.cliente_id == scalar_subq_user_id,
                self.tables.tb_control_download.c.dlgid_id == scalar_subq_dlgid_id
            )
        )

        for dlgid in l_dlgids:
            try:
                _ = self.bd.conn.execute(delstmt,{'username':auth.current_user(), 'dlgid':dlgid })    
            except Exception as ex:
                print(f'ApiQlik ERROR [reset_control_download] DATA EXCEPTION: {ex}')
                self.bd.rollback()
                self.bd.close()
                return False
        
        self.bd.conn.commit()
        self.bd.close()
        return True

"""
"""
class Read(Resource):
    ''' 
    Devuelve registros en modo csv sin marcarlos.
    Modificacion 2024-11-28:
    Uso el username en las consultas porque estas son personales de c/usuario
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

"""
"""
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
"""
"""
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
"""

api.add_resource( Ping, '/apiqlik/ping')
api.add_resource( Help, '/apiqlik/help')
api.add_resource( Dlgids, '/apiqlik/dlgids')
api.add_resource( DownloadDlgid, '/apiqlik/download_dlgid')
api.add_resource( Rollback, '/apiqlik/rollback')
api.add_resource( DownloadDlgidList, '/apiqlik/download_dlgid_list')
api.add_resource( Housekeeping, '/apiqlik/housekeeping')
#api.add_resource( Download, '/apiqlik/download')
#api.add_resource( Read, '/apiqlik/read')

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info( f'Starting APIQLIK' )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5022, debug=True)