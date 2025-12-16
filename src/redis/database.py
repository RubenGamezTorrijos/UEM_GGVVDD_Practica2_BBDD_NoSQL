# src/redis/database.py

import redis
import json
import time
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.client = None
        self.connect()
    
    def connect(self):
        """Conectar a Redis"""
        try:
            self.client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                password=os.getenv("REDIS_PASSWORD", "uem_password123"),
                decode_responses=True,
                socket_connect_timeout=5
            )
            
            # Verificar conexión
            self.client.ping()
            logger.info("Conexión a Redis establecida exitosamente")
            
        except redis.ConnectionError as e:
            logger.error(f"Error al conectar a Redis: {e}")
            raise
    
    def create_rankings(self, businesses_data: List[Dict]):
        """Crear rankings dinámicos usando Sorted Sets"""
        logger.info("Creando rankings en Redis...")
        
        # 1. Ranking global de negocios por puntuación
        for business in businesses_data:
            score = business.get('stars', 0) * 100  # Multiplicar para mayor precisión
            self.client.zadd(
                "ranking:business:global",
                {business['business_id']: score}
            )
            
            # Almacenar información completa del negocio
            business_key = f"business:{business['business_id']}"
            self.client.hset(business_key, mapping={
                'name': business.get('name', ''),
                'city': business.get('city', ''),
                'stars': str(business.get('stars', 0)),
                'review_count': str(business.get('review_count', 0))
            })
        
        # 2. Rankings por ciudad
        for business in businesses_data:
            city = business.get('city', 'unknown').lower().replace(' ', '_')
            score = business.get('stars', 0) * 100
            self.client.zadd(
                f"ranking:business:city:{city}",
                {business['business_id']: score}
            )
        
        # 3. Rankings por número de reseñas
        for business in businesses_data:
            review_count = business.get('review_count', 0)
            self.client.zadd(
                "ranking:business:popularity",
                {business['business_id']: review_count}
            )
        
        logger.info(f"Rankings creados para {len(businesses_data)} negocios")
    
    def update_ranking_on_new_review(self, review_data: Dict):
        """Actualizar rankings cuando se recibe una nueva reseña"""
        business_id = review_data['business_id']
        new_stars = review_data['stars']
        
        # Obtener negocio actual
        business_key = f"business:{business_id}"
        business_data = self.client.hgetall(business_key)
        
        if business_data:
            # Calcular nuevo promedio
            current_stars = float(business_data['stars'])
            current_reviews = int(business_data['review_count'])
            
            new_avg = ((current_stars * current_reviews) + new_stars) / (current_reviews + 1)
            
            # Actualizar datos del negocio
            self.client.hset(business_key, 'stars', str(new_avg))
            self.client.hset(business_key, 'review_count', str(current_reviews + 1))
            
            # Actualizar rankings
            new_score = new_avg * 100
            
            # Ranking global
            self.client.zadd("ranking:business:global", {business_id: new_score})
            
            # Ranking por ciudad
            city = business_data['city'].lower().replace(' ', '_')
            self.client.zadd(f"ranking:business:city:{city}", {business_id: new_score})
            
            # Ranking por popularidad
            self.client.zadd("ranking:business:popularity", {business_id: current_reviews + 1})
            
            logger.info(f"Rankings actualizados para negocio {business_id}")
            
            # Invalidar caché relacionada
            self.invalidate_cache(business_id, city)
            
            return new_avg
        
        return None
    
    def get_top_businesses(self, limit: int = 10, city: str = None) -> List[Dict]:
        """Obtener los mejores negocios"""
        if city:
            key = f"ranking:business:city:{city.lower().replace(' ', '_')}"
        else:
            key = "ranking:business:global"
        
        # Verificar si está en caché
        cache_key = f"cache:top_businesses:{key}:{limit}"
        cached = self.client.get(cache_key)
        
        if cached:
            logger.info(f"Resultados obtenidos de caché: {cache_key}")
            return json.loads(cached)
        
        # Obtener del sorted set
        business_ids = self.client.zrevrange(key, 0, limit - 1)
        
        results = []
        for business_id in business_ids:
            business_data = self.client.hgetall(f"business:{business_id}")
            if business_data:
                results.append({
                    'business_id': business_id,
                    **business_data
                })
        
        # Almacenar en caché por 5 minutos
        self.client.setex(cache_key, 300, json.dumps(results))
        
        return results
    
    def invalidate_cache(self, business_id: str = None, city: str = None):
        """Invalidar entradas de caché"""
        # Invalidar todas las cachés relacionadas con un negocio
        if business_id:
            pattern = f"cache:*{business_id}*"
            keys = self.client.keys(pattern)
            if keys:
                self.client.delete(*keys)
                logger.info(f"Cache invalidada para negocio {business_id}")
        
        # Invalidar cachés relacionadas con una ciudad
        if city:
            pattern = f"cache:*city:{city.lower().replace(' ', '_')}*"
            keys = self.client.keys(pattern)
            if keys:
                self.client.delete(*keys)
                logger.info(f"Cache invalidada para ciudad {city}")
    
    def benchmark_performance(self, mongo_results: List[Dict] = None):
        """Comparar rendimiento entre Redis y MongoDB"""
        logger.info("Realizando benchmark de rendimiento...")
        
        benchmarks = {}
        
        # Escenario 1: Obtener top 10 negocios globales
        start_time = time.time()
        redis_results = self.get_top_businesses(10)
        redis_time = time.time() - start_time
        
        benchmarks['top_10_global'] = {
            'redis_time': redis_time,
            'redis_results_count': len(redis_results)
        }
        
        # Escenario 2: Búsqueda por ciudad (con caché)
        city = "madrid"
        
        # Primera búsqueda (sin caché)
        start_time = time.time()
        self.get_top_businesses(10, city)
        first_call_time = time.time() - start_time
        
        # Segunda búsqueda (con caché)
        start_time = time.time()
        self.get_top_businesses(10, city)
        cached_call_time = time.time() - start_time
        
        benchmarks['city_search'] = {
            'without_cache': first_call_time,
            'with_cache': cached_call_time,
            'improvement': f"{(first_call_time - cached_call_time) / first_call_time * 100:.2f}%"
        }
        
        # Escenario 3: Actualización en tiempo real
        sample_review = {
            'business_id': 'uem_sample_1',
            'stars': 5.0,
            'user_id': 'uem_user_test'
        }
        
        start_time = time.time()
        self.update_ranking_on_new_review(sample_review)
        update_time = time.time() - start_time
        
        benchmarks['real_time_update'] = {
            'update_time': update_time
        }
        
        return benchmarks
    
    def implement_real_time_features(self):
        """Implementar características en tiempo real"""
        logger.info("Implementando características en tiempo real...")
        
        features = {}
        
        # 1. Contador de visitas en tiempo real
        business_id = "uem_sample_1"
        visits_key = f"business:{business_id}:visits:realtime"
        
        # Simular visitas
        for i in range(10):
            self.client.incr(visits_key)
            time.sleep(0.1)
        
        current_visits = self.client.get(visits_key)
        features['real_time_visits'] = {
            'business_id': business_id,
            'visits': current_visits
        }
        
        # 2. Tendencias del momento (última hora)
        trends_key = "trending:businesses:last_hour"
        
        # Simular actividad reciente
        recent_businesses = ['uem_sample_1', 'uem_sample_2', 'uem_sample_3']
        for business in recent_businesses:
            self.client.zincrby(trends_key, 1, business)
        
        # Obtener tendencias
        trending = self.client.zrevrange(trends_key, 0, 5, withscores=True)
        features['trending_now'] = trending
        
        # 3. Sesiones de usuario
        session_data = {
            'user_id': 'uem_user_1',
            'last_action': datetime.now().isoformat(),
            'page_views': 5,
            'searched_cities': ['madrid', 'barcelona']
        }
        
        session_key = f"session:uem_user_1"
        self.client.setex(session_key, 3600, json.dumps(session_data))  # 1 hora
        features['user_session'] = session_data
        
        return features
    
    def close(self):
        """Cerrar conexión"""
        if self.client:
            self.client.close()
            logger.info("Conexión a Redis cerrada")

# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    redis_mgr = RedisManager()
    
    try:
        # Datos de ejemplo
        sample_businesses = [
            {
                'business_id': 'uem_sample_1',
                'name': 'Sample Restaurant 1',
                'city': 'Madrid',
                'stars': 4.5,
                'review_count': 150
            },
            {
                'business_id': 'uem_sample_2',
                'name': 'Sample Restaurant 2',
                'city': 'Barcelona',
                'stars': 4.2,
                'review_count': 200
            }
        ]
        
        # Crear rankings
        redis_mgr.create_rankings(sample_businesses)
        
        # Obtener top negocios
        top_businesses = redis_mgr.get_top_businesses(5)
        print("\nTop 5 negocios globales:")
        for business in top_businesses:
            print(f"  {business['name']} - {business['stars']} estrellas")
        
        # Benchmark
        benchmarks = redis_mgr.benchmark_performance()
        print("\nBenchmark de rendimiento:")
        for test, data in benchmarks.items():
            print(f"\n{test}:")
            for key, value in data.items():
                print(f"  {key}: {value}")
        
        # Características en tiempo real
        features = redis_mgr.implement_real_time_features()
        print("\nCaracterísticas en tiempo real:")
        for feature, data in features.items():
            print(f"\n{feature}: {data}")
            
    finally:
        redis_mgr.close()