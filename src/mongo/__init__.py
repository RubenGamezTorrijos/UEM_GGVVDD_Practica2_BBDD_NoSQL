# src/mongo/__init__.py

"""
Módulo MongoDB para la práctica UEM NoSQL.
Contiene las funcionalidades para gestión de datos documentales.
"""

__version__ = "1.0.0"
__author__ = "UEM Student"

from .database import MongoDBManager
from .queries import MongoQueries
from .import_data import MongoDataImporter

__all__ = ['MongoDBManager', 'MongoQueries', 'MongoDataImporter']