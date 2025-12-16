# src/mongo/database.py

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure
import time
import logging
from datetime import datetime
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class MongoDBManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.connect()
    
    def connect(self):
        """Conectar a MongoDB"""
        try:
            connection_string = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/"
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión
            self.client.admin.command('ping')
            self.db = self.client[os.getenv('MONGO_DATABASE', 'yelp_mongo')]
            
            logger.info("Conexión a MongoDB establecida exitosamente")
            
        except ConnectionFailure as e:
            logger.error(f"Error al conectar a MongoDB: {e}")
            raise
    
    def import_data(self, collection_name: str, file_path: str):
        """Importar datos desde un archivo JSON"""
        import subprocess
        import os
        
        cmd = [
            'mongoimport',
            f'--host={os.getenv("MONGO_HOST")}',
            f'--port={os.getenv("MONGO_PORT")}',
            f'--username={os.getenv("MONGO_USER")}',
            f'--password={os.getenv("MONGO_PASSWORD")}',
            '--authenticationDatabase=admin',
            f'--db={os.getenv("MONGO_DATABASE")}',
            f'--collection={collection_name}',
            '--type=json',
            '--file=' + file_path,
            '--jsonArray'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"Datos importados a {collection_name}: {result.stdout}")
            else:
                logger.error(f"Error al importar: {result.stderr}")
        except Exception as e:
            logger.error(f"Error ejecutando mongoimport: {e}")
    
    # src/mongo/database.py

    def create_indexes(self, drop_existing: bool = True):
        """Crear índices para optimizar consultas"""
        logger.info("Creando índices...")
        
        try:
            # Lista de índices a crear con sus configuraciones
            indexes_to_create = [
                # Business indexes
                {
                    'collection': 'business',
                    'keys': [("business_id", ASCENDING)],
                    'name': "idx_business_business_id",
                    'unique': True
                },
                {
                    'collection': 'business',
                    'keys': [("city", ASCENDING)],
                    'name': "idx_business_city",
                    'unique': False
                },
                {
                    'collection': 'business',
                    'keys': [("stars", DESCENDING)],
                    'name': "idx_business_stars_desc",
                    'unique': False
                },
                {
                    'collection': 'business',
                    'keys': [("review_count", DESCENDING)],
                    'name': "idx_business_review_count_desc",
                    'unique': False
                },
                # User indexes
                {
                    'collection': 'user',
                    'keys': [("user_id", ASCENDING)],
                    'name': "idx_user_user_id",
                    'unique': True
                },
                {
                    'collection': 'user',
                    'keys': [("review_count", DESCENDING)],
                    'name': "idx_user_review_count_desc",
                    'unique': False
                },
                # Review indexes
                {
                    'collection': 'review',
                    'keys': [("review_id", ASCENDING)],
                    'name': "idx_review_review_id",
                    'unique': True
                },
                {
                    'collection': 'review',
                    'keys': [("business_id", ASCENDING)],
                    'name': "idx_review_business_id",
                    'unique': False
                },
                {
                    'collection': 'review',
                    'keys': [("user_id", ASCENDING)],
                    'name': "idx_review_user_id",
                    'unique': False
                },
                {
                    'collection': 'review',
                    'keys': [("stars", DESCENDING)],
                    'name': "idx_review_stars_desc",
                    'unique': False
                }
            ]
            
            # Crear índices
            created_count = 0
            for index_config in indexes_to_create:
                collection = self.db[index_config['collection']]
                
                try:
                    # Verificar si ya existe un índice con las mismas claves
                    existing_indexes = collection.index_information()
                    
                    # Buscar si ya existe un índice con las mismas claves
                    index_exists = False
                    for existing_name, existing_info in existing_indexes.items():
                        if existing_info['key'] == index_config['keys']:
                            index_exists = True
                            logger.debug(f"Índice ya existe: {existing_name} para {index_config['keys']}")
                            break
                    
                    if not index_exists or drop_existing:
                        # Si drop_existing es True y existe, eliminarlo primero
                        if drop_existing and index_exists:
                            # Buscar y eliminar el índice existente
                            for existing_name, existing_info in existing_indexes.items():
                                if existing_info['key'] == index_config['keys']:
                                    collection.drop_index(existing_name)
                                    logger.debug(f"Índice eliminado: {existing_name}")
                                    break
                        
                        # Crear nuevo índice
                        collection.create_index(
                            index_config['keys'],
                            name=index_config['name'],
                            unique=index_config.get('unique', False),
                            background=True  # Crear en segundo plano
                        )
                        created_count += 1
                        logger.debug(f"Índice creado: {index_config['name']}")
                        
                except Exception as e:
                    logger.warning(f"Error procesando índice {index_config['name']}: {e}")
                    # Continuar con el siguiente índice
            
            logger.info(f"Índices procesados: {created_count} creados/actualizados")
            return created_count
            
        except Exception as e:
            logger.error(f"Error creando índices: {e}")
            # No lanzar excepción, solo registrar error
            return 0
    
    def run_aggregation_queries(self):
        """Ejecutar consultas de agregación"""
        queries_results = {}
        
        # 1. Promedio de puntuación por ciudad
        logger.info("Ejecutando consulta: Promedio de puntuación por ciudad")
        start_time = time.time()
        
        pipeline = [
            {"$group": {
                "_id": "$city",
                "avg_stars": {"$avg": "$stars"},
                "business_count": {"$sum": 1}
            }},
            {"$sort": {"avg_stars": -1}},
            {"$limit": 10}
        ]
        
        result = list(self.db.business.aggregate(pipeline))
        queries_results['avg_stars_by_city'] = {
            'time': time.time() - start_time,
            'result': result
        }
        
        # 2. Top 5 negocios más valorados
        logger.info("Ejecutando consulta: Top 5 negocios más valorados")
        start_time = time.time()
        
        pipeline = [
            {"$match": {"review_count": {"$gt": 100}}},
            {"$sort": {"stars": -1, "review_count": -1}},
            {"$limit": 5},
            {"$project": {
                "_id": 0,
                "name": 1,
                "city": 1,
                "stars": 1,
                "review_count": 1
            }}
        ]
        
        result = list(self.db.business.aggregate(pipeline))
        queries_results['top_5_businesses'] = {
            'time': time.time() - start_time,
            'result': result
        }
        
        # 3. Usuarios con más reseñas escritas
        logger.info("Ejecutando consulta: Usuarios con más reseñas escritas")
        start_time = time.time()
        
        pipeline = [
            {"$sort": {"review_count": -1}},
            {"$limit": 10},
            {"$project": {
                "_id": 0,
                "user_id": 1,
                "name": 1,
                "review_count": 1,
                "yelping_since": 1
            }}
        ]
        
        result = list(self.db.user.aggregate(pipeline))
        queries_results['top_reviewers'] = {
            'time': time.time() - start_time,
            'result': result
        }
        
        # 4. Consulta con join implícito: Reseñas por ciudad
        logger.info("Ejecutando consulta: Reseñas por ciudad (lookup)")
        start_time = time.time()
        
        pipeline = [
            {"$lookup": {
                "from": "business",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "business_info"
            }},
            {"$unwind": "$business_info"},
            {"$group": {
                "_id": "$business_info.city",
                "avg_review_stars": {"$avg": "$stars"},
                "review_count": {"$sum": 1}
            }},
            {"$sort": {"review_count": -1}},
            {"$limit": 10}
        ]
        
        result = list(self.db.review.aggregate(pipeline))
        queries_results['reviews_by_city'] = {
            'time': time.time() - start_time,
            'result': result
        }
        
        return queries_results
    
    def benchmark_index_performance(self):
        """Comparar rendimiento con y sin índices"""
        logger.info("Realizando benchmark de índices...")
        
        benchmarks = {}
        
        # Deshabilitar temporalmente índices para la prueba
        self.db.business.drop_index([("business_id", ASCENDING)])
        
        # Consulta sin índice
        start_time = time.time()
        result = self.db.business.find_one({"business_id": "uem_sample_1"})
        time_without_index = time.time() - start_time
        
        # Crear índice
        self.db.business.create_index([("business_id", ASCENDING)])
        
        # Consulta con índice
        start_time = time.time()
        result = self.db.business.find_one({"business_id": "uem_sample_1"})
        time_with_index = time.time() - start_time
        
        benchmarks['business_id_lookup'] = {
            'without_index': time_without_index,
            'with_index': time_with_index,
            'improvement': f"{(time_without_index - time_with_index) / time_without_index * 100:.2f}%"
        }
        
        # Restaurar índice
        self.db.business.create_index([("business_id", ASCENDING)])
        
        return benchmarks
    
    def close(self):
        """Cerrar conexión"""
        if self.client:
            self.client.close()
            logger.info("Conexión a MongoDB cerrada")

# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    mongo = MongoDBManager()
    
    try:
        # Crear índices
        mongo.create_indexes()
        
        # Ejecutar consultas
        results = mongo.run_aggregation_queries()
        
        # Benchmark
        benchmarks = mongo.benchmark_index_performance()
        
        # Mostrar resultados
        for query_name, data in results.items():
            print(f"\n{query_name}:")
            print(f"Tiempo: {data['time']:.4f}s")
            for item in data['result'][:3]:  # Mostrar primeros 3 resultados
                print(f"  {item}")
        
        print("\nBenchmark de índices:")
        for test, data in benchmarks.items():
            print(f"{test}:")
            print(f"  Sin índice: {data['without_index']:.6f}s")
            print(f"  Con índice: {data['with_index']:.6f}s")
            print(f"  Mejora: {data['improvement']}")
            
    finally:
        mongo.close()