# src/redis/rankings.py

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class RedisRankings:
    """Clase para gestionar rankings en Redis"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
    
    def update_business_ranking(self, business_data: Dict) -> float:
        """
        Actualizar ranking de un negocio
        
        Args:
            business_data: Diccionario con datos del negocio
            
        Returns:
            float: Nuevo score del negocio
        """
        business_id = business_data.get('business_id')
        if not business_id:
            logger.error("business_id es requerido")
            return 0.0
        
        # Calcular score basado en múltiples factores
        stars = float(business_data.get('stars', 0))
        review_count = int(business_data.get('review_count', 0))
        is_open = int(business_data.get('is_open', 0))
        
        # Fórmula de score: stars * 100 + log10(review_count) * 10 + is_open * 5
        import math
        score = (stars * 100) + (math.log10(max(review_count, 1)) * 10) + (is_open * 5)
        
        # Actualizar ranking global
        self.redis.zadd("ranking:business:global", {business_id: score})
        
        # Actualizar ranking por ciudad
        city = business_data.get('city', 'unknown').lower().replace(' ', '_')
        if city != 'unknown':
            self.redis.zadd(f"ranking:business:city:{city}", {business_id: score})
        
        # Actualizar ranking por categoría
        categories = business_data.get('categories', [])
        if isinstance(categories, str):
            categories = [cat.strip() for cat in categories.split(',')]
        
        for category in categories[:3]:  # Máximo 3 categorías principales
            cat_key = category.lower().replace(' ', '_').replace('&', 'and')
            self.redis.zadd(f"ranking:business:category:{cat_key}", {business_id: score})
        
        # Guardar datos completos del negocio
        business_key = f"business:{business_id}"
        self.redis.hset(business_key, mapping={
            'name': business_data.get('name', ''),
            'city': business_data.get('city', ''),
            'state': business_data.get('state', ''),
            'stars': str(stars),
            'review_count': str(review_count),
            'is_open': str(is_open),
            'categories': json.dumps(categories) if categories else '[]',
            'last_updated': datetime.now().isoformat()
        })
        
        logger.debug(f"Ranking actualizado para {business_id}: score={score}")
        return score
    
    def get_top_businesses(self, ranking_type: str = "global", 
                          location: Optional[str] = None,
                          limit: int = 10,
                          with_scores: bool = False) -> List[Dict]:
        """
        Obtener los mejores negocios según ranking
        
        Args:
            ranking_type: Tipo de ranking (global, city, category)
            location: Ciudad o categoría específica
            limit: Número máximo de resultados
            with_scores: Incluir scores en la respuesta
            
        Returns:
            List[Dict]: Lista de negocios ordenados
        """
        # Determinar la clave del sorted set
        if ranking_type == "city" and location:
            key = f"ranking:business:city:{location.lower().replace(' ', '_')}"
        elif ranking_type == "category" and location:
            key = f"ranking:business:category:{location.lower().replace(' ', '_').replace('&', 'and')}"
        else:
            key = "ranking:business:global"
        
        # Obtener IDs de negocios ordenados
        if with_scores:
            results = self.redis.zrevrange(key, 0, limit - 1, withscores=True)
        else:
            results = self.redis.zrevrange(key, 0, limit - 1)
        
        businesses = []
        for item in results:
            if with_scores:
                business_id, score = item
            else:
                business_id = item
                score = self.redis.zscore(key, business_id) or 0
            
            # Obtener datos completos del negocio
            business_data = self.redis.hgetall(f"business:{business_id}")
            if business_data:
                business_info = {
                    'business_id': business_id,
                    'score': float(score),
                    **business_data
                }
                
                # Parsear categorías si existen
                if 'categories' in business_info:
                    try:
                        business_info['categories'] = json.loads(business_info['categories'])
                    except:
                        business_info['categories'] = []
                
                businesses.append(business_info)
        
        return businesses
    
    def update_ranking_on_review(self, review_data: Dict) -> Dict:
        """
        Actualizar rankings cuando se recibe una nueva reseña
        
        Args:
            review_data: Datos de la reseña
            
        Returns:
            Dict: Resultado de la actualización
        """
        business_id = review_data.get('business_id')
        new_stars = float(review_data.get('stars', 0))
        
        if not business_id:
            return {'error': 'business_id es requerido'}
        
        # Obtener datos actuales del negocio
        business_key = f"business:{business_id}"
        business_data = self.redis.hgetall(business_key)
        
        if not business_data:
            logger.warning(f"Negocio {business_id} no encontrado, creando nuevo...")
            business_data = {
                'stars': '0',
                'review_count': '0',
                'name': 'Unknown',
                'city': 'unknown'
            }
        
        # Calcular nuevo promedio
        current_stars = float(business_data.get('stars', 0))
        current_reviews = int(business_data.get('review_count', 0))
        
        if current_reviews > 0:
            new_avg = ((current_stars * current_reviews) + new_stars) / (current_reviews + 1)
        else:
            new_avg = new_stars
        
        # Preparar datos actualizados
        updated_data = {
            'stars': str(round(new_avg, 2)),
            'review_count': str(current_reviews + 1),
            'last_review': datetime.now().isoformat(),
            'review_count_updated': 'true'
        }
        
        # Actualizar datos del negocio
        self.redis.hset(business_key, mapping=updated_data)
        
        # Recalcular score
        business_data.update(updated_data)
        new_score = self.update_business_ranking(business_data)
        
        # Registrar la reseña para análisis temporal
        review_key = f"business:{business_id}:reviews"
        review_entry = {
            'stars': new_stars,
            'user_id': review_data.get('user_id', ''),
            'timestamp': datetime.now().isoformat(),
            'text_length': len(review_data.get('text', ''))
        }
        self.redis.lpush(review_key, json.dumps(review_entry))
        self.redis.ltrim(review_key, 0, 99)  # Mantener solo las 100 últimas
        
        # Actualizar trending (última hora)
        self._update_trending(business_id)
        
        return {
            'business_id': business_id,
            'new_average': new_avg,
            'new_score': new_score,
            'total_reviews': current_reviews + 1,
            'updated_at': datetime.now().isoformat()
        }
    
    def _update_trending(self, business_id: str):
        """Actualizar lista de tendencias"""
        trending_key = "trending:businesses:last_hour"
        
        # Incrementar score para este negocio
        self.redis.zincrby(trending_key, 1, business_id)
        
        # Establecer expiración si es la primera vez
        if self.redis.ttl(trending_key) == -1:  # No tiene expiración
            self.redis.expire(trending_key, 3600)  # 1 hora
    
    def get_trending_businesses(self, limit: int = 10) -> List[Dict]:
        """Obtener negocios en tendencia (última hora)"""
        trending_key = "trending:businesses:last_hour"
        
        results = self.redis.zrevrange(trending_key, 0, limit - 1, withscores=True)
        
        trending = []
        for business_id, trend_score in results:
            business_data = self.redis.hgetall(f"business:{business_id}")
            if business_data:
                trending.append({
                    'business_id': business_id,
                    'trend_score': int(trend_score),
                    'name': business_data.get('name', ''),
                    'city': business_data.get('city', '')
                })
        
        return trending
    
    def get_rank_position(self, business_id: str, ranking_type: str = "global", 
                         location: Optional[str] = None) -> Optional[int]:
        """Obtener posición en el ranking de un negocio"""
        if ranking_type == "city" and location:
            key = f"ranking:business:city:{location.lower().replace(' ', '_')}"
        elif ranking_type == "category" and location:
            key = f"ranking:business:category:{location.lower().replace(' ', '_').replace('&', 'and')}"
        else:
            key = "ranking:business:global"
        
        # Obtener posición (0-based)
        position = self.redis.zrevrank(key, business_id)
        return position + 1 if position is not None else None
    
    def get_ranking_stats(self) -> Dict:
        """Obtener estadísticas de los rankings"""
        stats = {}
        
        # Contar diferentes tipos de rankings
        pattern = "ranking:business:*"
        ranking_keys = self.redis.keys(pattern)
        
        stats['total_rankings'] = len(ranking_keys)
        stats['ranking_types'] = {}
        
        for key in ranking_keys:
            key_type = key.split(':')[2]  # city, category, global
            count = self.redis.zcard(key)
            stats['ranking_types'][key_type] = stats['ranking_types'].get(key_type, 0) + count
        
        # Estadísticas de negocios
        business_pattern = "business:*"
        business_keys = [k for k in self.redis.keys(business_pattern) if ':reviews' not in k and ':visits' not in k]
        stats['total_businesses'] = len(business_keys)
        
        # Score promedio
        if 'ranking:business:global' in ranking_keys:
            scores = self.redis.zrange("ranking:business:global", 0, -1, withscores=True)
            if scores:
                avg_score = sum(score for _, score in scores) / len(scores)
                stats['average_score'] = avg_score
        
        return stats
    
    def simulate_real_time_updates(self, num_updates: int = 10):
        """Simular actualizaciones en tiempo real para demostración"""
        logger.info(f"Simulando {num_updates} actualizaciones en tiempo real...")
        
        # Obtener algunos negocios existentes
        business_keys = [k for k in self.redis.keys("business:*") 
                        if ':reviews' not in k and ':visits' not in k]
        
        if not business_keys:
            logger.warning("No hay negocios para simular actualizaciones")
            return
        
        results = []
        for i in range(min(num_updates, len(business_keys))):
            business_id = business_keys[i].split(':')[1]
            
            # Crear reseña simulada
            import random
            review = {
                'business_id': business_id,
                'stars': random.choice([4.0, 4.5, 5.0]),  # Generalmente buenas reseñas
                'user_id': f"sim_user_{i}",
                'text': f"Simulated review {i}"
            }
            
            # Actualizar ranking
            result = self.update_ranking_on_review(review)
            results.append(result)
            
            # Pequeña pausa para simular tiempo real
            time.sleep(0.1)
        
        logger.info(f"Simulación completada: {len(results)} actualizaciones")
        return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    from database import RedisManager
    
    redis_mgr = RedisManager()
    rankings = RedisRankings(redis_mgr.client)
    
    try:
        # Crear algunos datos de prueba
        sample_businesses = [
            {
                'business_id': 'test_biz_1',
                'name': 'Test Restaurant 1',
                'city': 'Madrid',
                'stars': 4.5,
                'review_count': 100,
                'is_open': 1,
                'categories': ['Restaurants', 'Spanish']
            },
            {
                'business_id': 'test_biz_2',
                'name': 'Test Hotel 1',
                'city': 'Barcelona',
                'stars': 4.2,
                'review_count': 200,
                'is_open': 1,
                'categories': ['Hotels', 'Lodging']
            }
        ]
        
        # Actualizar rankings
        for business in sample_businesses:
            score = rankings.update_business_ranking(business)
            print(f"Negocio {business['name']} - Score: {score}")
        
        # Obtener top negocios
        top_businesses = rankings.get_top_businesses(limit=5)
        print(f"\nTop 5 negocios globales:")
        for biz in top_businesses:
            print(f"  {biz['name']} - {biz.get('city', 'N/A')} - Score: {biz.get('score', 0):.1f}")
        
        # Obtener estadísticas
        stats = rankings.get_ranking_stats()
        print(f"\nEstadísticas de rankings:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
            
    finally:
        redis_mgr.close()