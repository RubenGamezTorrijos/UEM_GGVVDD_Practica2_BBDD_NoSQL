# src/neo4j/__init__.py

"""
Módulo Neo4j para la práctica UEM NoSQL.
Contiene las funcionalidades para gestión de grafos y relaciones.
"""

__version__ = "1.0.0"
__author__ = "UEM Student"

from .database import Neo4jManager
from .queries import Neo4jQueries
from .import_data import Neo4jDataImporter

__all__ = ['Neo4jManager', 'Neo4jQueries', 'Neo4jDataImporter']