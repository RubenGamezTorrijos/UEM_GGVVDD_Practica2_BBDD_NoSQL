# src/utils/data_processor.py

import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

class DataProcessor:
    """Clase para procesamiento y transformación de datos"""
    
    @staticmethod
    def load_json_lines(file_path: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Cargar archivo JSON lines (uno por línea)
        
        Args:
            file_path: Ruta al archivo
            limit: Límite de líneas a cargar (None para todas)
            
        Returns:
            List[Dict]: Lista de diccionarios con los datos
        """
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if limit is not None and i >= limit:
                    break
                try:
                    data.append(json.loads(line.strip()))
                except json.JSONDecodeError as e:
                    logger.warning(f"Error en línea {i+1}: {e}")
        
        logger.info(f"Cargados {len(data)} registros de {file_path}")
        return data
    
    @staticmethod
    def save_json_lines(data: List[Dict], file_path: str):
        """Guardar datos en formato JSON lines"""
        with open(file_path, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
        
        logger.info(f"Guardados {len(data)} registros en {file_path}")
    
    @staticmethod
    def create_sample_dataset(original_file: str, sample_file: str, 
                            sample_size: int = 1000, random_state: int = 42) -> int:
        """
        Crear dataset de muestra desde archivo original
        
        Returns:
            int: Número de registros en la muestra
        """
        logger.info(f"Creando muestra de {sample_size} registros...")
        
        # Leer datos
        data = DataProcessor.load_json_lines(original_file, limit=sample_size * 2)
        
        if len(data) < sample_size:
            logger.warning(f"Solo hay {len(data)} registros disponibles")
            sample_data = data
        else:
            # Tomar muestra aleatoria
            import random
            random.seed(random_state)
            sample_data = random.sample(data, sample_size)
        
        # Guardar muestra
        DataProcessor.save_json_lines(sample_data, sample_file)
        
        return len(sample_data)
    
    @staticmethod
    def transform_for_mongodb(data: List[Dict], collection_type: str) -> List[Dict]:
        """Transformar datos para MongoDB"""
        transformed = []
        
        for item in data:
            # Transformaciones comunes para todos los tipos
            transformed_item = item.copy()
            
            # Convertir campos de fecha
            date_fields = ['date', 'yelping_since']
            for field in date_fields:
                if field in transformed_item and transformed_item[field]:
                    try:
                        transformed_item[field] = pd.to_datetime(
                            transformed_item[field]
                        ).isoformat()
                    except:
                        pass
            
            # Transformaciones específicas por tipo
            if collection_type == "business":
                # Asegurar que categories sea una lista
                if 'categories' in transformed_item:
                    if isinstance(transformed_item['categories'], str):
                        transformed_item['categories'] = [
                            cat.strip() for cat in transformed_item['categories'].split(',')
                        ]
                
                # Convertir atributos anidados
                if 'attributes' in transformed_item:
                    if isinstance(transformed_item['attributes'], str):
                        try:
                            transformed_item['attributes'] = json.loads(
                                transformed_item['attributes'].replace("'", '"')
                            )
                        except:
                            transformed_item['attributes'] = {}
            
            elif collection_type == "user":
                # Procesar lista de amigos
                if 'friends' in transformed_item:
                    if isinstance(transformed_item['friends'], str):
                        friends_list = [
                            f.strip() for f in transformed_item['friends'].split(',')
                            if f.strip() and f.strip() != 'None'
                        ]
                        transformed_item['friends'] = friends_list
                    elif transformed_item['friends'] is None:
                        transformed_item['friends'] = []
            
            transformed.append(transformed_item)
        
        return transformed
    
    @staticmethod
    def transform_for_neo4j_csv(data: List[Dict], entity_type: str) -> pd.DataFrame:
        """Transformar datos para Neo4j CSV"""
        if entity_type == "business":
            df = pd.DataFrame(data)
            
            # Seleccionar y renombrar columnas para Neo4j
            neo4j_df = pd.DataFrame()
            if 'business_id' in df.columns:
                neo4j_df['business_id:ID(Business)'] = df['business_id']
            if 'name' in df.columns:
                neo4j_df['name'] = df['name'].fillna('Unknown')
            if 'city' in df.columns:
                neo4j_df['city'] = df['city'].fillna('Unknown')
            if 'stars' in df.columns:
                neo4j_df['stars:float'] = df['stars'].fillna(0).astype(float)
            if 'review_count' in df.columns:
                neo4j_df['review_count:int'] = df['review_count'].fillna(0).astype(int)
            if 'categories' in df.columns:
                neo4j_df['categories'] = df['categories'].fillna('')
            
            neo4j_df[':LABEL'] = 'Business'
            
        elif entity_type == "user":
            df = pd.DataFrame(data)
            
            neo4j_df = pd.DataFrame()
            if 'user_id' in df.columns:
                neo4j_df['user_id:ID(User)'] = df['user_id']
            if 'name' in df.columns:
                neo4j_df['name'] = df['name'].fillna('Anonymous')
            if 'review_count' in df.columns:
                neo4j_df['review_count:int'] = df['review_count'].fillna(0).astype(int)
            if 'yelping_since' in df.columns:
                neo4j_df['yelping_since'] = df['yelping_since'].fillna('')
            if 'friends' in df.columns:
                # Guardar como string para procesar después
                neo4j_df['friends'] = df['friends'].apply(
                    lambda x: ','.join(x) if isinstance(x, list) else str(x)
                )
            
            neo4j_df[':LABEL'] = 'User'
        
        elif entity_type == "review":
            df = pd.DataFrame(data)
            
            neo4j_df = pd.DataFrame()
            if 'review_id' in df.columns:
                neo4j_df['review_id:ID(Review)'] = df['review_id']
            if 'user_id' in df.columns:
                neo4j_df[':START_ID(User)'] = df['user_id']
            if 'business_id' in df.columns:
                neo4j_df[':END_ID(Business)'] = df['business_id']
            if 'stars' in df.columns:
                neo4j_df['stars:float'] = df['stars'].fillna(0).astype(float)
            if 'date' in df.columns:
                neo4j_df['date:date'] = pd.to_datetime(
                    df['date'], errors='coerce'
                ).dt.strftime('%Y-%m-%d')
            if 'text' in df.columns:
                neo4j_df['text'] = df['text'].fillna('')
            
            neo4j_df[':TYPE'] = 'REVIEWED'
        
        else:
            raise ValueError(f"Tipo de entidad no soportado: {entity_type}")
        
        return neo4j_df
    
    @staticmethod
    def analyze_data_quality(data: List[Dict], data_type: str) -> Dict:
        """Analizar calidad de los datos"""
        if not data:
            return {"error": "No hay datos para analizar"}
        
        df = pd.DataFrame(data)
        
        analysis = {
            "total_records": len(df),
            "columns": list(df.columns),
            "missing_values": {},
            "data_types": {},
            "basic_stats": {}
        }
        
        # Analizar valores faltantes
        for column in df.columns:
            missing = df[column].isna().sum()
            if missing > 0:
                analysis["missing_values"][column] = {
                    "count": int(missing),
                    "percentage": round(missing / len(df) * 100, 2)
                }
        
        # Analizar tipos de datos
        for column in df.columns:
            dtype = str(df[column].dtype)
            analysis["data_types"][column] = dtype
            
            # Estadísticas básicas para columnas numéricas
            if pd.api.types.is_numeric_dtype(df[column]):
                analysis["basic_stats"][column] = {
                    "min": float(df[column].min()),
                    "max": float(df[column].max()),
                    "mean": float(df[column].mean()),
                    "std": float(df[column].std())
                }
        
        # Estadísticas específicas por tipo de datos
        if data_type == "business":
            if 'city' in df.columns:
                analysis["city_distribution"] = df['city'].value_counts().head(10).to_dict()
            if 'stars' in df.columns:
                analysis["stars_distribution"] = df['stars'].value_counts().sort_index().to_dict()
        
        elif data_type == "user":
            if 'review_count' in df.columns:
                analysis["review_count_stats"] = {
                    "users_with_reviews": int((df['review_count'] > 0).sum()),
                    "top_reviewer": int(df['review_count'].max()) if not df.empty else 0
                }
        
        return analysis
    
    @staticmethod
    def generate_data_hash(data: List[Dict]) -> str:
        """Generar hash MD5 de los datos para verificar integridad"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    @staticmethod
    def compare_datasets(dataset1: List[Dict], dataset2: List[Dict], 
                        key_field: str) -> Dict:
        """Comparar dos datasets"""
        df1 = pd.DataFrame(dataset1)
        df2 = pd.DataFrame(dataset2)
        
        comparison = {
            "dataset1_size": len(df1),
            "dataset2_size": len(df2),
            "common_keys": 0,
            "unique_to_dataset1": 0,
            "unique_to_dataset2": 0
        }
        
        if key_field in df1.columns and key_field in df2.columns:
            keys1 = set(df1[key_field].unique())
            keys2 = set(df2[key_field].unique())
            
            comparison["common_keys"] = len(keys1.intersection(keys2))
            comparison["unique_to_dataset1"] = len(keys1 - keys2)
            comparison["unique_to_dataset2"] = len(keys2 - keys1)
        
        return comparison

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Ejemplo de uso
    processor = DataProcessor()
    
    # Crear datos de ejemplo
    sample_data = [
        {
            "business_id": "test_1",
            "name": "Test Business",
            "city": "Madrid",
            "stars": 4.5,
            "review_count": 100,
            "categories": "Restaurants, Food",
            "date": "2023-01-15"
        },
        {
            "business_id": "test_2",
            "name": "Another Business",
            "city": "Barcelona",
            "stars": 3.8,
            "review_count": 50,
            "categories": "Shopping",
            "date": "2023-02-20"
        }
    ]
    
    # Transformar para MongoDB
    mongo_data = processor.transform_for_mongodb(sample_data, "business")
    print("Datos transformados para MongoDB:", mongo_data)
    
    # Transformar para Neo4j
    neo4j_df = processor.transform_for_neo4j_csv(sample_data, "business")
    print("\nDataFrame para Neo4j:")
    print(neo4j_df)
    
    # Analizar calidad
    analysis = processor.analyze_data_quality(sample_data, "business")
    print("\nAnálisis de calidad:")
    print(f"Total registros: {analysis['total_records']}")
    print(f"Columnas: {analysis['columns']}")
    
    # Generar hash
    data_hash = processor.generate_data_hash(sample_data)
    print(f"\nHash de datos: {data_hash}")