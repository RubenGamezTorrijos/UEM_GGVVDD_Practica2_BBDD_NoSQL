#!/usr/bin/env python3
# data-preparation.py

import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPreparer:
    def __init__(self, raw_data_path, processed_data_path):
        self.raw_path = Path(raw_data_path)
        self.processed_path = Path(processed_data_path)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
    def create_sample_dataset(self, sample_size=1000):
        """Crear un dataset de muestra para pruebas"""
        logger.info("Creando dataset de muestra...")
        
        # Datos de ejemplo para business
        businesses = []
        for i in range(sample_size):
            business = {
                "business_id": f"uem_sample_{i}",
                "name": f"Sample Business {i}",
                "address": f"Sample Address {i}",
                "city": f"City {i % 10}",
                "state": "SP",
                "postal_code": "28000",
                "latitude": 40.4168 + (i * 0.001),
                "longitude": -3.7038 + (i * 0.001),
                "stars": round(1 + (i % 4) + 0.5, 1),
                "review_count": i * 10,
                "is_open": 1,
                "categories": ["Restaurants", "Food"] if i % 2 == 0 else ["Shopping", "Retail"],
                "attributes": {
                    "BusinessAcceptsCreditCards": True,
                    "WiFi": "free" if i % 3 == 0 else "paid"
                },
                "hours": {
                    "Monday": "9:0-17:0",
                    "Tuesday": "9:0-17:0"
                }
            }
            businesses.append(business)
        
        # Guardar como JSON
        with open(self.processed_path / "business_sample.json", 'w') as f:
            for business in businesses:
                f.write(json.dumps(business) + '\n')
        
        logger.info(f"Muestra de {sample_size} negocios creada")
        
    def convert_to_csv_for_neo4j(self, json_file, entity_type):
        """Convertir JSON a CSV para Neo4j"""
        logger.info(f"Convirtiendo {json_file} a CSV para Neo4j...")
        
        data = []
        with open(self.raw_path / json_file, 'r') as f:
            for line in tqdm(f, desc=f"Procesando {json_file}"):
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        
        df = pd.DataFrame(data)
        
        if entity_type == "business":
            # Para nodos Business
            csv_data = df[['business_id', 'name', 'city', 'stars']].copy()
            csv_data.columns = ['business_id:ID(Business)', 'name', 'city', 'stars:float']
            csv_data[':LABEL'] = 'Business'
            
        elif entity_type == "review":
            # Para relaciones REVIEWED
            csv_data = df[['review_id', 'user_id', 'business_id', 'stars', 'date']].copy()
            csv_data.columns = [
                'review_id:ID(Review)',
                ':START_ID(User)',
                ':END_ID(Business)',
                'stars:float',
                'date:date'
            ]
            csv_data[':TYPE'] = 'REVIEWED'
            
        elif entity_type == "user":
            # Para nodos User
            csv_data = df[['user_id', 'name', 'review_count']].copy()
            csv_data.columns = ['user_id:ID(User)', 'name', 'review_count:int']
            csv_data[':LABEL'] = 'User'
        
        # Guardar CSV
        output_file = self.processed_path / f"{entity_type}_neo4j.csv"
        csv_data.to_csv(output_file, index=False)
        logger.info(f"CSV guardado en: {output_file}")
        
        return output_file
    
    def prepare_for_mongoimport(self, json_file):
        """Preparar archivos para mongoimport"""
        logger.info(f"Preparando {json_file} para MongoDB...")
        
        input_file = self.raw_path / json_file
        output_file = self.processed_path / f"mongo_{json_file}"
        
        # Para grandes archivos, procesar línea por línea
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in tqdm(infile, desc=f"Procesando {json_file}"):
                try:
                    data = json.loads(line)
                    # Convertir fechas a formato ISO
                    if 'date' in data:
                        data['date'] = pd.to_datetime(data['date']).isoformat()
                    outfile.write(json.dumps(data) + '\n')
                except:
                    continue
        
        logger.info(f"Archivo preparado: {output_file}")
        return output_file
    
    def generate_test_data(self):
        """Generar datos de prueba para todas las bases de datos"""
        logger.info("Generando datos de prueba completos...")
        
        # 1. Datos para MongoDB
        self.create_sample_dataset(5000)
        
        # 2. Datos para Neo4j
        users = []
        for i in range(100):
            user = {
                "user_id": f"uem_user_{i}",
                "name": f"User {i}",
                "review_count": i * 5,
                "yelping_since": "2015-01-01",
                "friends": [f"uem_user_{j}" for j in range(max(0, i-5), min(100, i+5)) if j != i]
            }
            users.append(user)
        
        with open(self.processed_path / "user_sample.json", 'w') as f:
            for user in users:
                f.write(json.dumps(user) + '\n')
        
        # 3. Datos para Redis (rankings)
        rankings = []
        for i in range(50):
            ranking = {
                "business_id": f"uem_sample_{i}",
                "name": f"Business {i}",
                "score": 1000 - (i * 10),
                "city": f"City {i % 5}",
                "category": "Restaurant" if i % 2 == 0 else "Shop"
            }
            rankings.append(ranking)
        
        with open(self.processed_path / "rankings_sample.json", 'w') as f:
            json.dump(rankings, f)
        
        logger.info("Datos de prueba generados exitosamente")

def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Preparar datos para la práctica NoSQL')
    parser.add_argument('--mode', choices=['sample', 'full', 'neo4j', 'mongo'],
                       default='sample', help='Modo de preparación')
    parser.add_argument('--size', type=int, default=1000,
                       help='Tamaño de la muestra (solo para modo sample)')
    
    args = parser.parse_args()
    
    preparer = DataPreparer('./data/raw', './data/processed')
    
    if args.mode == 'sample':
        preparer.generate_test_data()
    elif args.mode == 'neo4j':
        # Convertir datos reales para Neo4j
        preparer.convert_to_csv_for_neo4j('business.json', 'business')
        preparer.convert_to_csv_for_neo4j('review.json', 'review')
        preparer.convert_to_csv_for_neo4j('user.json', 'user')
    elif args.mode == 'mongo':
        # Preparar datos para MongoDB
        preparer.prepare_for_mongoimport('business.json')
        preparer.prepare_for_mongoimport('user.json')
        preparer.prepare_for_mongoimport('review.json')

if __name__ == "__main__":
    main()