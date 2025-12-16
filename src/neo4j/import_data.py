# src/neo4j/import_data.py

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import json
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

class Neo4jDataImporter:
    """Clase para importar datos a Neo4j"""
    
    def __init__(self, driver):
        self.driver = driver
    
    def import_businesses_from_csv(self, csv_path: str, batch_size: int = 1000) -> Dict:
        """Importar negocios desde CSV"""
        logger.info(f"Importando negocios desde {csv_path}...")
        
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL {
            WITH row
            CREATE (b:Business {
                business_id: row.`business_id:ID(Business)`,
                name: COALESCE(row.name, 'Unknown'),
                city: COALESCE(row.city, 'Unknown'),
                state: COALESCE(row.state, ''),
                stars: toFloat(COALESCE(row.`stars:float`, '0')),
                review_count: toInteger(COALESCE(row.`review_count:int`, '0')),
                categories: CASE 
                    WHEN row.categories IS NOT NULL THEN split(row.categories, ', ')
                    ELSE []
                END
            })
        } IN TRANSACTIONS OF $batch_size ROWS
        RETURN count(b) AS businesses_imported
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                file_path=f"file:///{csv_path}", 
                batch_size=batch_size
            )
            stats = result.single()
            
        logger.info(f"Negocios importados: {stats['businesses_imported']}")
        return dict(stats)
    
    def import_users_from_csv(self, csv_path: str, batch_size: int = 1000) -> Dict:
        """Importar usuarios desde CSV"""
        logger.info(f"Importando usuarios desde {csv_path}...")
        
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL {
            WITH row
            CREATE (u:User {
                user_id: row.`user_id:ID(User)`,
                name: COALESCE(row.name, 'Anonymous'),
                review_count: toInteger(COALESCE(row.`review_count:int`, '0')),
                yelping_since: COALESCE(row.yelping_since, ''),
                useful: toInteger(COALESCE(row.useful, '0')),
                funny: toInteger(COALESCE(row.funny, '0')),
                cool: toInteger(COALESCE(row.cool, '0')),
                elite: CASE 
                    WHEN row.elite IS NOT NULL THEN split(row.elite, ',')
                    ELSE []
                END,
                friends: CASE 
                    WHEN row.friends IS NOT NULL AND row.friends <> 'None' 
                    THEN split(row.friends, ',')
                    ELSE []
                END
            })
        } IN TRANSACTIONS OF $batch_size ROWS
        RETURN count(u) AS users_imported
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                file_path=f"file:///{csv_path}", 
                batch_size=batch_size
            )
            stats = result.single()
            
        logger.info(f"Usuarios importados: {stats['users_imported']}")
        return dict(stats)
    
    def import_reviews_from_csv(self, csv_path: str, batch_size: int = 500) -> Dict:
        """Importar reseñas y crear relaciones"""
        logger.info(f"Importando reseñas desde {csv_path}...")
        
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL {
            WITH row
            MATCH (u:User {user_id: row.`:START_ID(User)`})
            MATCH (b:Business {business_id: row.`:END_ID(Business)`})
            CREATE (u)-[r:REVIEWED {
                review_id: COALESCE(row.`review_id:ID(Review)`, ''),
                stars: toFloat(COALESCE(row.`stars:float`, '0')),
                date: date(COALESCE(row.`date:date`, '')),
                text: COALESCE(row.text, ''),
                useful: toInteger(COALESCE(row.useful, '0')),
                funny: toInteger(COALESCE(row.funny, '0')),
                cool: toInteger(COALESCE(row.cool, '0'))
            }]->(b)
        } IN TRANSACTIONS OF $batch_size ROWS
        RETURN count(r) AS reviews_imported
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                file_path=f"file:///{csv_path}", 
                batch_size=batch_size
            )
            stats = result.single()
            
        logger.info(f"Reseñas importadas: {stats['reviews_imported']}")
        return dict(stats)
    
    def create_friendship_relationships(self) -> Dict:
        """Crear relaciones de amistad entre usuarios"""
        logger.info("Creando relaciones de amistad...")
        
        query = """
        MATCH (u:User)
        WHERE size(u.friends) > 0
        UNWIND u.friends AS friend_id
        MATCH (friend:User {user_id: friend_id})
        MERGE (u)-[:FRIEND_OF]-(friend)
        RETURN count(*) AS friendships_created
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            stats = result.single()
            
        logger.info(f"Relaciones de amistad creadas: {stats['friendships_created']}")
        return dict(stats)
    
    def import_sample_data(self, data_dir: str = "./data/processed") -> Dict:
        """Importar datos de muestra a Neo4j"""
        results = {}
        data_path = Path(data_dir)
        
        # Limpiar base de datos existente
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Base de datos limpiada")
        
        # Importar datos de muestra
        sample_files = {
            'business': 'business_sample.csv',
            'user': 'user_sample.csv', 
            'review': 'review_sample.csv'
        }
        
        for entity, filename in sample_files.items():
            file_path = data_path / filename
            if file_path.exists():
                if entity == 'business':
                    results[entity] = self.import_businesses_from_csv(str(file_path))
                elif entity == 'user':
                    results[entity] = self.import_users_from_csv(str(file_path))
                elif entity == 'review':
                    results[entity] = self.import_reviews_from_csv(str(file_path))
        
        # Crear relaciones de amistad si hay datos de usuarios
        if 'user' in results:
            results['friendships'] = self.create_friendship_relationships()
        
        return results
    
    def validate_import(self) -> Dict:
        """Validar datos importados"""
        validation_stats = {}
        
        queries = {
            'total_nodes': "MATCH (n) RETURN count(n) AS total_nodes",
            'total_relationships': "MATCH ()-[r]->() RETURN count(r) AS total_relationships",
            'node_types': """
                MATCH (n) 
                RETURN labels(n)[0] AS node_type, count(n) AS count 
                ORDER BY count DESC
            """,
            'relationship_types': """
                MATCH ()-[r]->() 
                RETURN type(r) AS rel_type, count(r) AS count 
                ORDER BY count DESC
            """,
            'business_stats': """
                MATCH (b:Business) 
                RETURN avg(b.stars) AS avg_stars, 
                       max(b.stars) AS max_stars,
                       min(b.stars) AS min_stars,
                       count(b) AS business_count
            """,
            'user_stats': """
                MATCH (u:User) 
                RETURN avg(u.review_count) AS avg_reviews,
                       max(u.review_count) AS max_reviews,
                       count(u) AS user_count
            """
        }
        
        with self.driver.session() as session:
            for stat_name, query in queries.items():
                result = session.run(query)
                data = result.single()
                if data:
                    validation_stats[stat_name] = dict(data)
                else:
                    result = session.run(query)
                    validation_stats[stat_name] = [dict(record) for record in result]
        
        logger.info("Validación completada")
        return validation_stats
    
    def generate_sample_data_for_testing(self, output_dir: str = "./data/processed") -> None:
        """Generar datos de muestra para pruebas"""
        import random
        from datetime import datetime, timedelta
        
        data_path = Path(output_dir)
        data_path.mkdir(parents=True, exist_ok=True)
        
        # Generar negocios de muestra
        businesses = []
        cities = ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao']
        categories = ['Restaurants', 'Shopping', 'Hotels', 'Bars', 'Museums']
        
        for i in range(100):
            business = {
                'business_id:ID(Business)': f"uem_biz_{i}",
                'name': f"Business {i}",
                'city': random.choice(cities),
                'stars:float': round(random.uniform(1.0, 5.0), 1),
                'review_count:int': random.randint(0, 500),
                'categories': ', '.join(random.sample(categories, random.randint(1, 3)))
            }
            businesses.append(business)
        
        # Generar usuarios de muestra
        users = []
        for i in range(50):
            user = {
                'user_id:ID(User)': f"uem_user_{i}",
                'name': f"User {i}",
                'review_count:int': random.randint(0, 100),
                'yelping_since': '2015-01-01',
                'friends': ','.join([f"uem_user_{j}" for j in random.sample(range(50), random.randint(0, 5))])
            }
            users.append(user)
        
        # Generar reseñas de muestra
        reviews = []
        for i in range(200):
            review = {
                'review_id:ID(Review)': f"uem_rev_{i}",
                ':START_ID(User)': f"uem_user_{random.randint(0, 49)}",
                ':END_ID(Business)': f"uem_biz_{random.randint(0, 99)}",
                'stars:float': random.choice([1.0, 2.0, 3.0, 4.0, 5.0]),
                'date:date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
                'text': f"Sample review text {i}"
            }
            reviews.append(review)
        
        # Guardar archivos CSV
        pd.DataFrame(businesses).to_csv(data_path / "business_sample.csv", index=False)
        pd.DataFrame(users).to_csv(data_path / "user_sample.csv", index=False)
        pd.DataFrame(reviews).to_csv(data_path / "review_sample.csv", index=False)
        
        logger.info(f"Datos de muestra generados en {output_dir}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    from database import Neo4jManager
    
    neo4j = Neo4jManager()
    importer = Neo4jDataImporter(neo4j.driver)
    
    try:
        # Generar datos de muestra si no existen
        sample_dir = "./data/processed"
        if not (Path(sample_dir) / "business_sample.csv").exists():
            importer.generate_sample_data_for_testing(sample_dir)
        
        # Importar datos de muestra
        results = importer.import_sample_data(sample_dir)
        print("Resultados de importación:", results)
        
        # Validar importación
        stats = importer.validate_import()
        print("\nEstadísticas de la base de datos:")
        for key, value in stats.items():
            print(f"{key}: {value}")
            
    finally:
        neo4j.close()