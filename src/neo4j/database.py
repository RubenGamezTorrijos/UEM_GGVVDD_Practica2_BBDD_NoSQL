# src/neo4j/database.py

from neo4j import GraphDatabase, basic_auth
import logging
import pandas as pd
from typing import Dict, List, Any
import os
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
            
            # Verificar conexión
            with self.driver.session() as session:
                result = session.run("RETURN 'Connected' AS message")
                logger.info("Conexión a Neo4j establecida exitosamente")
                
        except Exception as e:
            logger.error(f"Error al conectar a Neo4j: {e}")
            raise
    
    def import_data(self, nodes_file: str, relationships_file: str = None):
        """Importar datos desde archivos CSV"""
        with self.driver.session() as session:
            
            # Importar nodos Business
            if "business" in nodes_file:
                logger.info("Importando nodos Business...")
                
                query = """
                LOAD CSV WITH HEADERS FROM $file_path AS row
                CREATE (b:Business {
                    business_id: row.`business_id:ID(Business)`,
                    name: row.name,
                    city: row.city,
                    stars: toFloat(row.`stars:float`)
                })
                """
                
                session.run(query, file_path=f"file:///{nodes_file}")
                logger.info(f"Nodos Business importados desde {nodes_file}")
            
            # Importar nodos User (si existen)
            elif "user" in nodes_file:
                logger.info("Importando nodos User...")
                
                query = """
                LOAD CSV WITH HEADERS FROM $file_path AS row
                CREATE (u:User {
                    user_id: row.`user_id:ID(User)`,
                    name: row.name,
                    review_count: toInteger(row.`review_count:int`)
                })
                """
                
                session.run(query, file_path=f"file:///{nodes_file}")
                logger.info(f"Nodos User importados desde {nodes_file}")
    
    def create_graph_relationships(self, reviews_file: str):
        """Crear relaciones REVIEWED entre usuarios y negocios"""
        logger.info("Creando relaciones REVIEWED...")
        
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        MATCH (u:User {user_id: row.`:START_ID(User)`})
        MATCH (b:Business {business_id: row.`:END_ID(Business)`})
        CREATE (u)-[r:REVIEWED {
            review_id: row.`review_id:ID(Review)`,
            stars: toFloat(row.`stars:float`),
            date: date(row.`date:date`)
        }]->(b)
        """
        
        with self.driver.session() as session:
            session.run(query, file_path=f"file:///{reviews_file}")
        
        logger.info("Relaciones REVIEWED creadas")
    
    def run_cypher_queries(self):
        """Ejecutar consultas Cypher"""
        queries_results = {}
        
        with self.driver.session() as session:
            
            # 1. Buscar usuarios que han visitado los mismos negocios
            logger.info("Ejecutando consulta: Usuarios con negocios en común")
            
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
            
            # 2. Negocios más "centrales" en la red (grado de conexión)
            logger.info("Ejecutando consulta: Negocios más centrales")
            
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
            
            # 3. Rutas más cortas entre usuarios (gustos similares)
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
            
            # 4. Negocios mejor valorados por categoría implícita
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
            
            # 5. Análisis de comunidades (usuarios que frecuentan las mismas zonas)
            logger.info("Ejecutando consulta: Análisis de comunidades por ciudad")
            
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
            
            # Patrón 1: Densidad de conexiones
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
            patterns['graph_density'] = dict(result.single())
            
            # Patrón 2: Distribución de grados (conexiones por nodo)
            query = """
            MATCH (u:User)-[r:REVIEWED]->()
            WITH u, COUNT(r) AS degree
            RETURN avg(degree) AS avg_user_degree,
                   min(degree) AS min_user_degree,
                   max(degree) AS max_user_degree,
                   stDev(degree) AS std_user_degree
            """
            
            result = session.run(query)
            patterns['user_degree_distribution'] = dict(result.single())
            
            # Patrón 3: Distribución de ratings
            query = """
            MATCH ()-[r:REVIEWED]->()
            RETURN avg(r.stars) AS avg_rating,
                   min(r.stars) AS min_rating,
                   max(r.stars) AS max_rating,
                   count(r) AS total_ratings
            """
            
            result = session.run(query)
            patterns['rating_distribution'] = dict(result.single())
        
        return patterns
    
    def close(self):
        """Cerrar conexión"""
        if self.driver:
            self.driver.close()
            logger.info("Conexión a Neo4j cerrada")

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