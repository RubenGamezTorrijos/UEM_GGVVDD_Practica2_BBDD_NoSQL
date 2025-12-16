# src/neo4j/queries.py

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class Neo4jQueries:
    """Clase para gestionar consultas Cypher específicas"""
    
    def __init__(self, driver):
        self.driver = driver
    
    def find_users_with_common_businesses(self, min_common: int = 3, limit: int = 10) -> List[Dict]:
        """Encontrar usuarios que han visitado los mismos negocios"""
        query = """
        MATCH (u1:User)-[r1:REVIEWED]->(b:Business)<-[r2:REVIEWED]-(u2:User)
        WHERE u1.user_id < u2.user_id
        WITH u1, u2, COLLECT(DISTINCT b.name) AS common_businesses,
             AVG(r1.stars) AS avg_rating1, AVG(r2.stars) AS avg_rating2
        WHERE SIZE(common_businesses) >= $min_common
        RETURN u1.name AS user1_name,
               u1.user_id AS user1_id,
               u2.name AS user2_name,
               u2.user_id AS user2_id,
               SIZE(common_businesses) AS common_count,
               common_businesses,
               avg_rating1,
               avg_rating2,
               ABS(avg_rating1 - avg_rating2) AS rating_difference
        ORDER BY common_count DESC, rating_difference ASC
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                min_common=min_common, 
                limit=limit
            )
            return [dict(record) for record in result]
    
    def find_most_central_businesses(self, limit: int = 10) -> List[Dict]:
        """Encontrar negocios más centrales en la red (grado de conexión)"""
        query = """
        MATCH (b:Business)<-[r:REVIEWED]-()
        WITH b, COUNT(r) AS degree, COLLECT(DISTINCT r.stars) AS ratings
        OPTIONAL MATCH (b)<-[r2:REVIEWED]-(u:User)
        WITH b, degree, ratings,
             COUNT(DISTINCT u) AS unique_users,
             AVG(r2.stars) AS avg_rating,
             stDev(r2.stars) AS rating_std
        RETURN b.name AS business_name,
               b.city AS city,
               degree AS connection_degree,
               unique_users,
               avg_rating,
               rating_std,
               [rating IN ratings | toFloat(rating)] AS all_ratings
        ORDER BY degree DESC, unique_users DESC
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [dict(record) for record in result]
    
    def find_shortest_path_between_users(self, user1_id: str, user2_id: str, max_depth: int = 5) -> Optional[Dict]:
        """Encontrar la ruta más corta entre dos usuarios"""
        query = """
        MATCH (u1:User {user_id: $user1_id})
        MATCH (u2:User {user_id: $user2_id})
        MATCH path = shortestPath((u1)-[*1..5]-(u2))
        WHERE ALL(r IN relationships(path) WHERE type(r) = 'REVIEWED')
        WITH path, 
             nodes(path) AS path_nodes,
             relationships(path) AS path_rels
        RETURN 
            [node IN path_nodes | 
                CASE 
                    WHEN labels(node)[0] = 'User' THEN 'User: ' + node.name
                    WHEN labels(node)[0] = 'Business' THEN 'Business: ' + node.name
                    ELSE labels(node)[0]
                END
            ] AS path_description,
            SIZE(path_nodes) - 1 AS path_length,
            [rel IN path_rels | 
                {type: type(rel), stars: rel.stars, review_id: rel.review_id}
            ] AS relationships_info
        LIMIT 1
        """
        
        with self.driver.session() as session:
            result = session.run(
                query, 
                user1_id=user1_id, 
                user2_id=user2_id
            )
            record = result.single()
            return dict(record) if record else None
    
    def find_business_recommendations(self, user_id: str, limit: int = 5) -> List[Dict]:
        """Encontrar recomendaciones de negocios para un usuario"""
        query = """
        MATCH (target:User {user_id: $user_id})-[r1:REVIEWED]->(visited:Business)
        WITH target, COLLECT(visited) AS visited_businesses
        
        // Encontrar usuarios similares
        MATCH (similar:User)-[r2:REVIEWED]->(shared:Business)<-[r3:REVIEWED]-(target)
        WHERE similar <> target AND NOT shared IN visited_businesses
        WITH target, similar, visited_businesses,
             COUNT(DISTINCT shared) AS common_count,
             AVG(r3.stars) AS target_avg, AVG(r2.stars) AS similar_avg
        WHERE common_count >= 2 AND ABS(target_avg - similar_avg) <= 1.0
        
        // Encontrar negocios que usuarios similares han visitado pero el target no
        MATCH (similar)-[r:REVIEWED]->(recommended:Business)
        WHERE NOT recommended IN visited_businesses
        AND r.stars >= 4.0
        
        RETURN recommended.name AS business_name,
               recommended.city AS city,
               recommended.stars AS business_rating,
               r.stars AS similar_user_rating,
               similar.name AS recommended_by,
               common_count,
               recommended.business_id AS business_id
        ORDER BY r.stars DESC, common_count DESC
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, limit=limit)
            return [dict(record) for record in result]
    
    # src/neo4j/queries.py

    def analyze_network_communities(self) -> List[Dict]:
        """Analizar comunidades en la red"""
        query = """
        // Primero eliminar el grafo si ya existe
        CALL gds.graph.exists('businessNetwork') YIELD exists
        WHERE exists
        CALL gds.graph.drop('businessNetwork') YIELD graphName
        RETURN graphName
        """
        
        try:
            with self.driver.session() as session:
                # Intentar eliminar grafo existente
                session.run(query)
                
                # Crear nuevo grafo
                create_query = """
                CALL gds.graph.project(
                  'businessNetwork',
                  ['User', 'Business'],
                  {
                    REVIEWED: {
                      orientation: 'UNDIRECTED',
                      properties: 'stars'
                    }
                  }
                )
                YIELD graphName, nodeCount, relationshipCount
                RETURN graphName
                """
                
                session.run(create_query)
                
                # Ejecutar algoritmo de comunidades
                community_query = """
                CALL gds.louvain.stream('businessNetwork')
                YIELD nodeId, communityId
                RETURN communityId, COUNT(nodeId) AS community_size
                ORDER BY community_size DESC
                LIMIT 10
                """
                
                result = session.run(community_query)
                return [dict(record) for record in result]
                
        except Exception as e:
            logger.warning(f"Algoritmo GDS no disponible o con error: {e}")
            return self._fallback_community_analysis()
    
    def _fallback_community_analysis(self) -> List[Dict]:
        """Análisis alternativo de comunidades"""
        query = """
        MATCH (u:User)-[:REVIEWED]->(b:Business)
        WITH u, b.city AS city, COUNT(*) AS visits
        ORDER BY visits DESC
        WITH u, COLLECT(city)[0] AS primary_city
        
        WITH primary_city, COLLECT(u) AS users
        RETURN primary_city AS community_label,
               SIZE(users) AS community_size,
               [u IN users | u.name][0..3] AS sample_users
        ORDER BY community_size DESC
        LIMIT 10
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]
    
    def find_influential_users(self, limit: int = 10) -> List[Dict]:
        """Encontrar usuarios influyentes en la red"""
        query = """
        MATCH (u:User)-[r:REVIEWED]->(b:Business)
        WITH u, COUNT(r) AS total_reviews, AVG(r.stars) AS avg_rating
        WHERE total_reviews >= 10
        
        // Calcular influencia basada en conexiones
        OPTIONAL MATCH (u)-[:REVIEWED]->(b1:Business)<-[:REVIEWED]-(other:User)
        WITH u, total_reviews, avg_rating, COUNT(DISTINCT other) AS influenced_users
        
        RETURN u.name AS user_name,
               u.user_id AS user_id,
               total_reviews,
               avg_rating,
               influenced_users,
               (total_reviews * 0.4 + influenced_users * 0.6) AS influence_score
        ORDER BY influence_score DESC
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [dict(record) for record in result]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    from database import Neo4jManager
    
    neo4j = Neo4jManager()
    queries = Neo4jQueries(neo4j.driver)
    
    try:
        # Ejecutar consultas de ejemplo
        print("1. Negocios más centrales:")
        central = queries.find_most_central_businesses(5)
        for biz in central[:3]:
            print(f"  {biz['business_name']} - Grado: {biz['connection_degree']}")
        
        print("\n2. Usuarios con negocios en común:")
        common = queries.find_users_with_common_businesses(2, 5)
        for item in common[:3]:
            print(f"  {item['user1_name']} y {item['user2_name']}: {item['common_count']} negocios")
        
        print("\n3. Usuarios influyentes:")
        influential = queries.find_influential_users(5)
        for user in influential[:3]:
            print(f"  {user['user_name']}: Score {user['influence_score']:.2f}")
            
    finally:
        neo4j.close()