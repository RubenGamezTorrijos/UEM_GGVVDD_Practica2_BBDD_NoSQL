# src/neo4j/database.py

from neo4j import GraphDatabase, basic_auth
import logging
import pandas as pd
from typing import Dict, List, Any
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class Neo4jManager:
    def __init__(self):
        self.driver = None
        self.connect()
    
    def connect(self):
        """Conectar a Neo4j"""
        try:
            uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            user = os.getenv("NEO4J_USER", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "uem_password123")
            
            self.driver = GraphDatabase.driver(
                uri,
                auth=basic_auth(user, password)
            )
            
            # Verificar conexion
            with self.driver.session() as session:
                result = session.run("RETURN 'Connected' AS message")
                logger.info("Conexion a Neo4j establecida exitosamente")
                
        except Exception as e:
            logger.error(f"Error al conectar a Neo4j: {e}")
            raise
    
    def import_data(self, nodes_file: str, relationships_file: str = None):
        """Importar datos desde archivos CSV usando pandas"""
        # Construir ruta completa al archivo
        csv_path = Path(__file__).parent.parent.parent / "data" / "processed" / nodes_file
        
        if not csv_path.exists():
            logger.error(f"Archivo no encontrado: {csv_path}")
            return
            
        logger.info(f"Leyendo {csv_path}...")
        df = pd.read_csv(csv_path)
        
        with self.driver.session() as session:
            
            # Importar nodos Business
            if "business" in nodes_file:
                logger.info(f"Importando {len(df)} nodos Business...")
                
                for idx, row in df.iterrows():
                    query = """
                    CREATE (b:Business {
                        business_id: $business_id,
                        name: $name,
                        city: $city,
                        stars: $stars
                    })
                    """
                    session.run(query, 
                        business_id=str(row['business_id:ID(Business)']),
                        name=str(row['name']),
                        city=str(row['city']),
                        stars=float(row['stars:float'])
                    )
                
                logger.info(f"Nodos Business importados: {len(df)} nodos creados")
            
            # Importar nodos User
            elif "user" in nodes_file:
                logger.info(f"Importando {len(df)} nodos User...")
                
                for idx, row in df.iterrows():
                    query = """
                    CREATE (u:User {
                        user_id: $user_id,
                        name: $name,
                        review_count: $review_count
                    })
                    """
                    session.run(query,
                        user_id=str(row['user_id:ID(User)']),
                        name=str(row['name']),
                        review_count=int(row['review_count:int'])
                    )
                
                logger.info(f"Nodos User importados: {len(df)} nodos creados")
    
    def create_graph_relationships(self, reviews_file: str):
        """Crear relaciones REVIEWED entre usuarios y negocios"""
        logger.info("Creando relaciones REVIEWED...")
        
        # Construir ruta completa al archivo
        csv_path = Path(__file__).parent.parent.parent / "data" / "processed" / reviews_file
        
        if not csv_path.exists():
            logger.error(f"Archivo no encontrado: {csv_path}")
            return
            
        logger.info(f"Leyendo {csv_path}...")
        df = pd.read_csv(csv_path)
        
        logger.info(f"Creando {len(df)} relaciones...")
        
        with self.driver.session() as session:
            for idx, row in df.iterrows():
                query = """
                MATCH (u:User {user_id: $user_id})
                MATCH (b:Business {business_id: $business_id})
                CREATE (u)-[r:REVIEWED {
                    review_id: $review_id,
                    stars: $stars,
                    date: date($date)
                }]->(b)
                """
                session.run(query,
                    user_id=str(row[':START_ID(User)']),
                    business_id=str(row[':END_ID(Business)']),
                    review_id=str(row['review_id:ID(Review)']),
                    stars=float(row['stars:float']),
                    date=str(row['date:date'])
                )
        
        logger.info(f"Relaciones REVIEWED creadas: {len(df)} relaciones")
    
    def run_cypher_queries(self):
        """Ejecutar consultas Cypher"""
        queries_results = {}
        
        with self.driver.session() as session:
            
            # 1. Buscar usuarios que han visitado los mismos negocios
            logger.info("Ejecutando consulta: Usuarios con negocios en comun")
            
            query = """
            MATCH (u1:User)-[:REVIEWED]->(b:Business)<-[:REVIEWED]-(u2:User)
            WHERE u1.user_id < u2.user_id
            WITH u1, u2, COUNT(b) AS common_businesses
            WHERE common_businesses > 2
            RETURN u1.name AS user1, u2.name AS user2, common_businesses
            ORDER BY common_businesses DESC
            LIMIT 10
            """
            
            result = session.run(query)
            queries_results['users_with_common_businesses'] = [
                dict(record) for record in result
            ]
            
            # 2. Negocios mas "centrales" en la red (grado de conexion)
            logger.info("Ejecutando consulta: Negocios mas centrales")
            
            query = """
            MATCH (b:Business)<-[r:REVIEWED]-()
            RETURN b.name AS business_name,
                   b.city AS city,
                   COUNT(r) AS review_count,
                   AVG(r.stars) AS avg_rating
            ORDER BY review_count DESC
            LIMIT 10
            """
            
            result = session.run(query)
            queries_results['most_central_businesses'] = [
                dict(record) for record in result
            ]
            
            # 3. Rutas mas cortas entre usuarios (gustos similares)
            logger.info("Ejecutando consulta: Rutas entre usuarios similares")
            
            query = """
            MATCH (u1:User {user_id: $user1}), (u2:User {user_id: $user2})
            MATCH path = shortestPath((u1)-[:REVIEWED*]-(u2))
            RETURN path
            LIMIT 3
            """
            
            # Usar usuarios de ejemplo
            result = session.run(query, user1="uem_user_1", user2="uem_user_5")
            queries_results['shortest_paths'] = [
                dict(record) for record in result
            ]
            
            # 4. Negocios mejor valorados por categoria implicita
            logger.info("Ejecutando consulta: Negocios mejor valorados")
            
            query = """
            MATCH (b:Business)
            WHERE b.stars >= 4.0
            OPTIONAL MATCH (b)<-[r:REVIEWED]-()
            WITH b, COUNT(r) AS review_count, AVG(r.stars) AS avg_user_rating
            RETURN b.name AS name,
                   b.city AS city,
                   b.stars AS business_rating,
                   avg_user_rating,
                   review_count
            ORDER BY avg_user_rating DESC
            LIMIT 10
            """
            
            result = session.run(query)
            queries_results['top_rated_businesses'] = [
                dict(record) for record in result
            ]
            
            # 5. Analisis de comunidades (usuarios que frecuentan las mismas zonas)
            logger.info("Ejecutando consulta: Analisis de comunidades por ciudad")
            
            query = """
            MATCH (u:User)-[:REVIEWED]->(b:Business)
            WITH u, b.city AS city, COUNT(*) AS visits
            ORDER BY visits DESC
            WITH u, COLLECT(city)[0] AS favorite_city
            RETURN favorite_city, COUNT(u) AS users_count
            ORDER BY users_count DESC
            LIMIT 10
            """
            
            result = session.run(query)
            queries_results['user_communities_by_city'] = [
                dict(record) for record in result
            ]
        
        return queries_results
    
    def visualize_graph_patterns(self):
        """Analizar y describir patrones en el grafo"""
        logger.info("Analizando patrones del grafo...")
        
        patterns = {}
        
        with self.driver.session() as session:
            
            # Patron 1: Densidad de conexiones
            query = """
            MATCH (u:User)
            WITH COUNT(u) AS user_count
            MATCH (b:Business)
            WITH user_count, COUNT(b) AS business_count
            MATCH ()-[r:REVIEWED]->()
            RETURN user_count, business_count, COUNT(r) AS total_reviews,
                   COUNT(r) * 1.0 / (user_count * business_count) AS connection_density
            """
            
            result = session.run(query)
            record = result.single()
            patterns['graph_density'] = dict(record) if record else {
                'user_count': 0, 'business_count': 0, 'total_reviews': 0, 'connection_density': 0
            }
            
            # Patron 2: Distribucion de grados (conexiones por nodo)
            query = """
            MATCH (u:User)-[r:REVIEWED]->()
            WITH u, COUNT(r) AS degree
            RETURN avg(degree) AS avg_user_degree,
                   min(degree) AS min_user_degree,
                   max(degree) AS max_user_degree,
                   stDev(degree) AS std_user_degree
            """
            
            result = session.run(query)
            record = result.single()
            patterns['user_degree_distribution'] = dict(record) if record else {
                'avg_user_degree': 0, 'min_user_degree': 0, 'max_user_degree': 0, 'std_user_degree': 0
            }
            
            # Patron 3: Distribucion de ratings
            query = """
            MATCH ()-[r:REVIEWED]->()
            RETURN avg(r.stars) AS avg_rating,
                   min(r.stars) AS min_rating,
                   max(r.stars) AS max_rating,
                   count(r) AS total_ratings
            """
            
            result = session.run(query)
            record = result.single()
            patterns['rating_distribution'] = dict(record) if record else {
                'avg_rating': 0, 'min_rating': 0, 'max_rating': 0, 'total_ratings': 0
            }
        
        return patterns
    
    def close(self):
        """Cerrar conexion"""
        if self.driver:
            self.driver.close()
            logger.info("Conexion a Neo4j cerrada")

# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    neo4j = Neo4jManager()
    
    try:
        # Ejecutar consultas
        results = neo4j.run_cypher_queries()
        
        # Analizar patrones
        patterns = neo4j.visualize_graph_patterns()
        
        # Mostrar resultados
        for query_name, data in results.items():
            print(f"\n{query_name}:")
            for item in data[:5]:  # Mostrar primeros 5 resultados
                print(f"  {item}")
        
        print("\nPatrones del grafo:")
        for pattern_name, data in patterns.items():
            print(f"\n{pattern_name}:")
            for key, value in data.items():
                print(f"  {key}: {value}")
                
    finally:
        neo4j.close()