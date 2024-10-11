#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
Defino las tablas
"""

from datetime import datetime
from sqlalchemy import Column, Integer, Double, Float, DateTime, String
from sqlalchemy import Table, Column, MetaData, ForeignKey

metadata = MetaData()

tb_datos = Table('datos', metadata,
    Column('id', Integer(), primary_key=True),
    Column('fechadata', DateTime(), default=datetime.now, onupdate=datetime.now ),
    Column('fechasys', DateTime(), default=datetime.now, onupdate=datetime.now ),
    Column('dlgid', String(20), nullable=False, index=True),
    Column('tag', String(20), index=True),
    Column('valor', Float())
    )

tb_control_acceso = Table('control_acceso', metadata,
    Column('id', Integer(), primary_key=True),
    Column('username', String(50), nullable=False, unique=True),
    Column('password', String(50), nullable=False ),
    Column('last_row', Integer())
    )

tb_equipos_validos = Table('equipos_validos', metadata,
    Column('id', Integer(), primary_key=True),
    Column('dlgid', String(20),nullable=False, unique=True, index=True)
)

