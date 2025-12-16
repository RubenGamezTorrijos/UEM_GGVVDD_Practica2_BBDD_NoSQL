# src/mongo/import_data.py

import subprocess
import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from .database import MongoDBManager

logger = logging.getLogger(__name__)

class MongoDataImporter:
    """Clase para importar datos a MongoDB"""
    
    def __init__(self, mongo_manager: Optional[MongoDBManager] = None):
        self.mongo = mongo_manager or MongoDBManager()
    
    def import_json_file(self, collection_name: str, file_path: str, 
                        drop_existing: bool = False) -> bool:
        """
        Importar archivo JSON a MongoDB usando mongoimport
        
        Args:
            collection_name: Nombre de la colección
            file_path: Ruta al archivo JSON
            drop_existing: Si True, elimina la colección existente primero
            
        Returns:
            bool: True si la importación fue exitosa
        """
        if not Path(file_path).exists():
            logger.error(f"Archivo no encontrado: {file_path}")
            return False
        
        if drop_existing and collection_name in self.mongo.db.list_collection_names():
            logger.info(f"Eliminando colección existente: {collection_name}")
            self.mongo.db[collection_name].drop()
        
        # Construir comando mongoimport
        cmd = [
            'mongoimport',
            '--host', os.getenv('MONGO_HOST', 'localhost'),
            '--port', os.getenv('MONGO_PORT', '27017'),
            '--username', os.getenv('MONGO_USER', 'admin'),
            '--password', os.getenv('MONGO_PASSWORD', 'uem_password123'),
            '--authenticationDatabase', 'admin',
            '--db', os.getenv('MONGO_DATABASE', 'yelp_mongo'),
            '--collection', collection_name,
            '--file', file_path,
            '--jsonArray',
            '--numInsertionWorkers', '4'
        ]
        
        logger.info(f"Importando {file_path} a colección {collection_name}...")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"Importación exitosa: {result.stdout}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error en mongoimport: {e.stderr}")
            return False
        except FileNotFoundError:
            logger.error("mongoimport no encontrado. Asegúrate de tener MongoDB instalado localmente.")
            return self._import_using_python(file_path, collection_name)
    
    def _import_using_python(self, file_path: str, collection_name: str) -> bool:
        """Importar usando PyMongo (más lento pero no requiere mongoimport)"""
        import json
        from tqdm import tqdm
        
        logger.info("Usando PyMongo para importación...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = [json.loads(line) for line in tqdm(f, desc="Leyendo archivo")]
            
            if data:
                self.mongo.db[collection_name].insert_many(data)
                logger.info(f"Importados {len(data)} documentos a {collection_name}")
                return True
            
        except Exception as e:
            logger.error(f"Error importando con PyMongo: {e}")
        
        return False
    
    def import_sample_data(self, data_dir: str = "./data/samples") -> Dict[str, bool]:
        """Importar datos de muestra a MongoDB"""
        results: Dict[str, bool] = {}
        data_path = Path(data_dir)
        
        # Importar negocios
        business_file = data_path / "business_sample.json"
        if business_file.exists():
            results['business'] = self.import_json_file(
                "business", str(business_file), drop_existing=True
            )
        
        # Importar usuarios
        user_file = data_path / "user_sample.json"
        if user_file.exists():
            results['user'] = self.import_json_file(
                "user", str(user_file), drop_existing=False
            )
        
        # Importar rankings para pruebas
        rankings_file = data_path / "rankings_sample.json"
        if rankings_file.exists():
            results['rankings'] = self.import_json_file(
                "rankings", str(rankings_file), drop_existing=False
            )
        
        return results
    
    def validate_import(self, collection_name: str) -> Dict[str, Any]:
        """Validar datos importados"""
        collection = self.mongo.db[collection_name]
        
        stats: Dict[str, Any] = {
            "count": collection.count_documents({}),
            "indexes": list(collection.index_information().keys()),
            "sample_document": collection.find_one()
        }
        
        # Estadísticas adicionales para ciertas colecciones
        if collection_name == "business":
            pipeline = [
                {"$group": {
                    "_id": None,
                    "avg_stars": {"$avg": "$stars"},
                    "total_reviews": {"$sum": "$review_count"},
                    "unique_cities": {"$addToSet": "$city"}
                }}
            ]
            result = list(collection.aggregate(pipeline))
            if result:
                stats.update(result[0])
                stats['unique_cities_count'] = len(stats.get('unique_cities', []))
        
        logger.info(f"Validación de {collection_name}: {stats['count']} documentos")
        return stats

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    importer = MongoDataImporter()
    
    # Importar datos de muestra
    results = importer.import_sample_data()
    print("Resultados de importación:", results)
    
    # Validar importación
    for collection in ['business', 'user']:
        if collection in results and results[collection]:
            stats = importer.validate_import(collection)
            print(f"\nEstadísticas de {collection}:")
            for key, value in stats.items():
                if key != 'sample_document':
                    print(f"  {key}: {value}")