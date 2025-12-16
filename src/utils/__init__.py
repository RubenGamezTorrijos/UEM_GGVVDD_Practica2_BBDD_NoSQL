# src/utils/__init__.py

"""
Módulo de utilidades para la práctica UEM NoSQL.
Contiene funciones auxiliares para procesamiento y análisis.
"""

__version__ = "1.0.0"
__author__ = "UEM Student"

from .data_processor import DataProcessor
from .performance import PerformanceBenchmark

__all__ = ['DataProcessor', 'PerformanceBenchmark']