# src/redis/cache.py

import logging
import json
import hashlib
import time
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from functools import wraps

logger = logging.getLogger(__name__)

class RedisCache:
    """Clase para gestión de caché con Redis"""
    
    def __init__(self, redis_client, prefix: str = "cache"):
        self.redis = redis_client
        self.prefix = prefix
    
    def _generate_cache_key(self, function_name: str, *args, **kwargs) -> str:
        """Generar clave de caché única basada en función y argumentos"""
        # Crear string de argumentos
        args_str = str(args) + str(sorted(kwargs.items()))
        
        # Generar hash MD5
        key_hash = hashlib.md5(args_str.encode()).hexdigest()
        
        return f"{self.prefix}:{function_name}:{key_hash}"
    
    def cache_result(self, ttl: int = 300):
        """
        Decorador para cachear resultados de funciones
        
        Args:
            ttl: Time To Live en segundos (default: 5 minutos)
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generar clave de caché
                cache_key = self._generate_cache_key(func.__name__, *args, **kwargs)
                
                # Intentar obtener de caché
                cached = self.redis.get(cache_key)
                if cached is not None:
                    logger.debug(f"Cache hit: {cache_key}")
                    
                    # Registrar estadística
                    self._record_cache_hit(cache_key)
                    
                    try:
                        return json.loads(cached)
                    except json.JSONDecodeError:
                        # Si el cache está corrupto, continuar con la ejecución
                        logger.warning(f"Cache corrupto para {cache_key}")
                
                # Ejecutar función y cachear resultado
                logger.debug(f"Cache miss: {cache_key}")
                result = func(*args, **kwargs)
                
                # Cachear resultado
                try:
                    self.redis.setex(cache_key, ttl, json.dumps(result))
                    
                    # Registrar estadística
                    self._record_cache_miss(cache_key, len(json.dumps(result)))
                    
                except Exception as e:
                    logger.error(f"Error cacheando resultado: {e}")
                
                return result
            
            return wrapper
        return decorator
    
    def _record_cache_hit(self, cache_key: str):
        """Registrar un hit de caché"""
        stats_key = f"{self.prefix}:stats:hits"
        self.redis.incr(stats_key)
        
        # Registrar hit por tipo de caché
        cache_type = cache_key.split(':')[1] if ':' in cache_key else 'unknown'
        type_key = f"{self.prefix}:stats:hits:{cache_type}"
        self.redis.incr(type_key)
    
    def _record_cache_miss(self, cache_key: str, data_size: int):
        """Registrar un miss de caché y tamaño de datos"""
        stats_key = f"{self.prefix}:stats:misses"
        self.redis.incr(stats_key)
        
        # Registrar tamaño de datos
        size_key = f"{self.prefix}:stats:data_size"
        self.redis.incrby(size_key, data_size)
        
        # Registrar miss por tipo de caché
        cache_type = cache_key.split(':')[1] if ':' in cache_key else 'unknown'
        type_key = f"{self.prefix}:stats:misses:{cache_type}"
        self.redis.incr(type_key)
    
    def get_cached(self, cache_key: str) -> Optional[Any]:
        """Obtener valor de caché directamente"""
        cached = self.redis.get(cache_key)
        if cached:
            try:
                return json.loads(cached)
            except json.JSONDecodeError:
                return None
        return None
    
    def set_cached(self, cache_key: str, value: Any, ttl: int = 300) -> bool:
        """Establecer valor en caché directamente"""
        try:
            self.redis.setex(cache_key, ttl, json.dumps(value))
            return True
        except Exception as e:
            logger.error(f"Error estableciendo caché: {e}")
            return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidar todas las entradas de caché que coincidan con el patrón
        
        Returns:
            int: Número de claves eliminadas
        """
        full_pattern = f"{self.prefix}:{pattern}"
        keys = self.redis.keys(full_pattern)
        
        if keys:
            count = self.redis.delete(*keys)
            logger.info(f"Invalidadas {count} entradas de caché con patrón: {pattern}")
            return count
        
        return 0
    
    def invalidate_function_cache(self, function_name: str) -> int:
        """Invalidar todas las entradas de caché de una función específica"""
        pattern = f"{function_name}:*"
        return self.invalidate_pattern(pattern)
    
    def clear_all(self) -> int:
        """Limpiar toda la caché"""
        pattern = "*"
        return self.invalidate_pattern(pattern)
    
    def get_cache_stats(self) -> Dict:
        """Obtener estadísticas de la caché"""
        stats = {}
        
        # Estadísticas básicas
        stats_keys = [
            f"{self.prefix}:stats:hits",
            f"{self.prefix}:stats:misses",
            f"{self.prefix}:stats:data_size"
        ]
        
        for key in stats_keys:
            value = self.redis.get(key)
            stats[key.split(':')[-1]] = int(value) if value else 0
        
        # Calcular hit rate
        total = stats.get('hits', 0) + stats.get('misses', 0)
        stats['hit_rate'] = stats['hits'] / total if total > 0 else 0
        
        # Estadísticas por tipo
        pattern = f"{self.prefix}:stats:*:*"
        type_keys = self.redis.keys(pattern)
        
        stats_by_type = {}
        for key in type_keys:
            parts = key.split(':')
            if len(parts) >= 4:
                stat_type = parts[-2]  # hits o misses
                cache_type = parts[-1]  # tipo de caché
                
                if cache_type not in stats_by_type:
                    stats_by_type[cache_type] = {'hits': 0, 'misses': 0}
                
                value = self.redis.get(key)
                stats_by_type[cache_type][stat_type] = int(value) if value else 0
        
        stats['by_type'] = stats_by_type
        
        # Información de memoria
        try:
            memory_info = self.redis.info('memory')
            stats['memory_used'] = memory_info.get('used_memory', 0)
            stats['memory_peak'] = memory_info.get('used_memory_peak', 0)
        except:
            stats['memory_info'] = 'No disponible'
        
        return stats
    
    def cache_expensive_query(self, query_func: Callable, query_name: str, 
                            *args, ttl: int = 600, **kwargs) -> Any:
        """
        Cachear una consulta costosa con nombre específico
        
        Args:
            query_func: Función que ejecuta la consulta
            query_name: Nombre identificativo de la consulta
            ttl: Time To Live en segundos
            *args, **kwargs: Argumentos para la función
            
        Returns:
            Resultado cacheado o nuevo
        """
        cache_key = f"{self.prefix}:query:{query_name}"
        
        # Añadir argumentos al key si existen
        if args or kwargs:
            args_hash = hashlib.md5((str(args) + str(kwargs)).encode()).hexdigest()
            cache_key += f":{args_hash}"
        
        # Intentar obtener de caché
        cached = self.get_cached(cache_key)
        if cached is not None:
            logger.info(f"Consulta cacheada encontrada: {query_name}")
            return cached
        
        # Ejecutar consulta
        logger.info(f"Ejecutando consulta costosa: {query_name}")
        start_time = time.time()
        result = query_func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        logger.info(f"Consulta {query_name} tomó {execution_time:.2f} segundos")
        
        # Cachear resultado
        self.set_cached(cache_key, result, ttl)
        
        # Registrar tiempo de ejecución
        perf_key = f"{self.prefix}:perf:{query_name}"
        self.redis.rpush(perf_key, execution_time)
        self.redis.ltrim(perf_key, 0, 99)  # Mantener últimos 100 registros
        
        return result
    
    def monitor_performance(self, query_name: str) -> Dict:
        """Monitorear rendimiento de consultas específicas"""
        perf_key = f"{self.prefix}:perf:{query_name}"
        times = self.redis.lrange(perf_key, 0, -1)
        
        if not times:
            return {}
        
        times = [float(t) for t in times]
        
        return {
            'query_name': query_name,
            'samples': len(times),
            'avg_time': sum(times) / len(times),
            'min_time': min(times),
            'max_time': max(times),
            'last_10_avg': sum(times[-10:]) / min(10, len(times)) if times else 0
        }
    
    def warm_up_cache(self, warmup_data: Dict[str, Any], ttl: int = 3600):
        """
        Pre-calentar la caché con datos conocidos
        
        Args:
            warmup_data: Diccionario con claves y valores para cachear
            ttl: Time To Live en segundos
        """
        logger.info(f"Pre-calentando caché con {len(warmup_data)} elementos...")
        
        for key, value in warmup_data.items():
            cache_key = f"{self.prefix}:warmup:{key}"
            self.set_cached(cache_key, value, ttl)
        
        logger.info("Pre-calentamiento completado")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    from database import RedisManager
    
    redis_mgr = RedisManager()
    cache = RedisCache(redis_mgr.client, prefix="uem_cache")
    
    try:
        # Ejemplo de uso del decorador
        @cache.cache_result(ttl=60)
        def expensive_calculation(n: int) -> int:
            print(f"Calculando para n={n}...")
            time.sleep(1)  # Simular cálculo costoso
            return n * n
        
        # Primera llamada (cache miss)
        print("Primera llamada:")
        result1 = expensive_calculation(5)
        print(f"Resultado: {result1}")
        
        # Segunda llamada (cache hit)
        print("\nSegunda llamada (debería ser desde caché):")
        result2 = expensive_calculation(5)
        print(f"Resultado: {result2}")
        
        # Estadísticas de caché
        stats = cache.get_cache_stats()
        print(f"\nEstadísticas de caché:")
        print(f"Hits: {stats.get('hits', 0)}")
        print(f"Misses: {stats.get('misses', 0)}")
        print(f"Hit Rate: {stats.get('hit_rate', 0):.1%}")
        
        # Ejemplo de cache_expensive_query
        def complex_query(param: str) -> Dict:
            print(f"Ejecutando query compleja con param={param}...")
            time.sleep(2)
            return {"result": f"data for {param}", "timestamp": datetime.now().isoformat()}
        
        print("\nEjemplo de cache_expensive_query:")
        query_result = cache.cache_expensive_query(complex_query, "complex_query", "test_param")
        print(f"Resultado: {query_result}")
        
        # Monitoreo de rendimiento
        perf = cache.monitor_performance("complex_query")
        print(f"\nRendimiento de complex_query: {perf}")
            
    finally:
        redis_mgr.close()