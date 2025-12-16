# src/utils/performance.py

import time
import logging
from typing import Dict, List, Any, Callable
from datetime import datetime
import statistics
import json

logger = logging.getLogger(__name__)

class PerformanceBenchmark:
    """Clase para benchmarking y comparación de rendimiento"""
    
    def __init__(self):
        self.results = {}
    
    def benchmark_function(self, func: Callable, func_name: str, 
                          *args, iterations: int = 100, **kwargs) -> Dict:
        """
        Realizar benchmark de una función
        
        Args:
            func: Función a benchmarkear
            func_name: Nombre de la función
            iterations: Número de iteraciones
            *args, **kwargs: Argumentos para la función
            
        Returns:
            Dict: Resultados del benchmark
        """
        logger.info(f"Benchmarking {func_name} ({iterations} iteraciones)...")
        
        times = []
        results = []
        
        for i in range(iterations):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            
            times.append(end_time - start_time)
            
            # Guardar resultado de la primera iteración
            if i == 0:
                results.append(result)
        
        # Calcular estadísticas
        stats = {
            'function': func_name,
            'iterations': iterations,
            'total_time': sum(times),
            'avg_time': statistics.mean(times),
            'min_time': min(times),
            'max_time': max(times),
            'std_time': statistics.stdev(times) if len(times) > 1 else 0,
            'times_per_iteration': times[:10],  # Solo primeros 10 para referencia
            'sample_result': results[0] if results else None
        }
        
        self.results[func_name] = stats
        logger.info(f"{func_name}: avg={stats['avg_time']:.6f}s, min={stats['min_time']:.6f}s, max={stats['max_time']:.6f}s")
        
        return stats
    
    def compare_systems(self, mongo_manager, neo4j_manager, redis_manager) -> Dict:
        """
        Comparar rendimiento entre los tres sistemas de bases de datos
        
        Returns:
            Dict: Resultados de la comparación
        """
        logger.info("Comparando rendimiento entre sistemas NoSQL...")
        
        comparison = {
            'timestamp': datetime.now().isoformat(),
            'systems': {}
        }
        
        # Consulta 1: Obtener top 10 negocios
        comparison['query_1_top_businesses'] = self._compare_top_businesses_query(
            mongo_manager, neo4j_manager, redis_manager
        )
        
        # Consulta 2: Buscar por ciudad
        comparison['query_2_city_search'] = self._compare_city_search_query(
            mongo_manager, neo4j_manager, redis_manager
        )
        
        # Consulta 3: Análisis de relaciones
        comparison['query_3_relationship_analysis'] = self._compare_relationship_query(
            mongo_manager, neo4j_manager, redis_manager
        )
        
        # Consulta 4: Actualización en tiempo real
        comparison['query_4_real_time_update'] = self._compare_update_query(
            mongo_manager, neo4j_manager, redis_manager
        )
        
        # Resumen comparativo
        comparison['summary'] = self._generate_comparison_summary(comparison)
        
        return comparison
    
    def _compare_top_businesses_query(self, mongo, neo4j, redis) -> Dict:
        """Comparar consulta de mejores negocios"""
        results = {}
        
        # MongoDB
        def mongo_top_businesses():
            return list(mongo.db.business.find()
                       .sort("stars", -1)
                       .limit(10))
        
        mongo_stats = self.benchmark_function(
            mongo_top_businesses, 
            "mongo_top_businesses",
            iterations=50
        )
        results['mongodb'] = mongo_stats
        
        # Neo4j
        def neo4j_top_businesses():
            with neo4j.driver.session() as session:
                query = """
                MATCH (b:Business)
                RETURN b.name, b.stars, b.city
                ORDER BY b.stars DESC
                LIMIT 10
                """
                result = session.run(query)
                return [dict(record) for record in result]
        
        neo4j_stats = self.benchmark_function(
            neo4j_top_businesses,
            "neo4j_top_businesses",
            iterations=50
        )
        results['neo4j'] = neo4j_stats
        
        # Redis
        if redis:
            def redis_top_businesses():
                return redis.get_top_businesses(10)
            
            redis_stats = self.benchmark_function(
                redis_top_businesses,
                "redis_top_businesses",
                iterations=50
            )
            results['redis'] = redis_stats
        else:
            results['redis'] = {'error': 'Redis unavailable'}
        
        return results
    
    def _compare_city_search_query(self, mongo, neo4j, redis) -> Dict:
        """Comparar búsqueda por ciudad"""
        results = {}
        city = "madrid"
        
        # MongoDB
        def mongo_city_search():
            return list(mongo.db.business.find({"city": city})
                       .sort("stars", -1)
                       .limit(10))
        
        results['mongodb'] = self.benchmark_function(
            mongo_city_search, "mongo_city_search", iterations=30
        )
        
        # Neo4j
        def neo4j_city_search():
            with neo4j.driver.session() as session:
                query = """
                MATCH (b:Business {city: $city})
                RETURN b.name, b.stars
                ORDER BY b.stars DESC
                LIMIT 10
                """
                result = session.run(query, city=city.title())
                return [dict(record) for record in result]
        
        results['neo4j'] = self.benchmark_function(
            neo4j_city_search, "neo4j_city_search", iterations=30
        )
        
        # Redis
        if redis:
            def redis_city_search():
                return redis.get_top_businesses(10, city)
            
            results['redis'] = self.benchmark_function(
                redis_city_search, "redis_city_search", iterations=30
            )
        else:
            results['redis'] = {'error': 'Redis unavailable'}
        
        return results
    
    def _compare_relationship_query(self, mongo, neo4j, redis) -> Dict:
        """Comparar consulta de relaciones"""
        results = {}
        
        # MongoDB (simulación con lookup)
        def mongo_relationships():
            pipeline = [
                {
                    "$lookup": {
                        "from": "review",
                        "localField": "business_id",
                        "foreignField": "business_id",
                        "as": "reviews"
                    }
                },
                {
                    "$unwind": "$reviews"
                },
                {
                    "$group": {
                        "_id": "$business_id",
                        "name": {"$first": "$name"},
                        "review_count": {"$sum": 1},
                        "avg_rating": {"$avg": "$reviews.stars"}
                    }
                },
                {
                    "$sort": {"review_count": -1}
                },
                {
                    "$limit": 10
                }
            ]
            return list(mongo.db.business.aggregate(pipeline))
        
        results['mongodb'] = self.benchmark_function(
            mongo_relationships, "mongo_relationships", iterations=20
        )
        
        # Neo4j (nativo para relaciones)
        def neo4j_relationships():
            with neo4j.driver.session() as session:
                query = """
                MATCH (b:Business)<-[r:REVIEWED]-()
                RETURN b.name, COUNT(r) AS review_count, AVG(r.stars) AS avg_rating
                ORDER BY review_count DESC
                LIMIT 10
                """
                result = session.run(query)
                return [dict(record) for record in result]
        
        results['neo4j'] = self.benchmark_function(
            neo4j_relationships, "neo4j_relationships", iterations=20
        )
        
        # Redis (no aplica directamente, usar datos precomputados)
        def redis_relationships():
            # En Redis, las relaciones se manejan de manera diferente
            return {"message": "Redis no es óptimo para consultas de relaciones complejas"}
        
        results['redis'] = self.benchmark_function(
            redis_relationships, "redis_relationships", iterations=20
        )
        
        return results
    
    def _compare_update_query(self, mongo, neo4j, redis) -> Dict:
        """Comparar actualización en tiempo real"""
        results = {}
        
        # Datos de prueba para actualización
        test_review = {
            "review_id": "test_review_" + str(int(time.time())),
            "business_id": "test_business",
            "user_id": "test_user",
            "stars": 4.5,
            "date": datetime.now().isoformat()
        }
        
        # MongoDB
        def mongo_update():
            # Generar ID único para esta iteración
            current_review = test_review.copy()
            current_review["review_id"] = f"{test_review['review_id']}_{time.perf_counter()}"
            
            # Insertar nueva reseña
            mongo.db.review.insert_one(current_review)
            
            # Actualizar contador en negocio
            mongo.db.business.update_one(
                {"business_id": current_review["business_id"]},
                {
                    "$inc": {"review_count": 1},
                    "$set": {"last_updated": datetime.now()}
                }
            )
            return True
        
        results['mongodb'] = self.benchmark_function(
            mongo_update, "mongo_update", iterations=20
        )
        
        # Neo4j
        def neo4j_update():
            # Generar ID único
            current_review = test_review.copy()
            current_review["review_id"] = f"{test_review['review_id']}_{time.perf_counter()}"
            
            with neo4j.driver.session() as session:
                query = """
                MATCH (u:User {user_id: $user_id})
                MATCH (b:Business {business_id: $business_id})
                CREATE (u)-[r:REVIEWED {
                    review_id: $review_id,
                    stars: $stars,
                    date: date($date)
                }]->(b)
                """
                session.run(query, **current_review)
            return True
        
        results['neo4j'] = self.benchmark_function(
            neo4j_update, "neo4j_update", iterations=20
        )
        
        # Redis
        if redis:
            def redis_update():
                redis.update_ranking_on_new_review(test_review)
                return True
            
            results['redis'] = self.benchmark_function(
                redis_update, "redis_update", iterations=20
            )
        else:
            results['redis'] = {'error': 'Redis unavailable'}
        
        return results
    
    def _generate_comparison_summary(self, comparison_results: Dict) -> Dict:
        """Generar resumen comparativo"""
        summary = {
            'best_performing': {},
            'performance_ratios': {},
            'recommendations': []
        }
        
        # Analizar cada consulta
        for query_name, query_results in comparison_results.items():
            if query_name == 'summary':
                continue
            
            if not isinstance(query_results, dict):
                continue
            
            # Encontrar el más rápido para cada consulta
            fastest_system = None
            fastest_time = float('inf')
            
            for system, stats in query_results.items():
                if isinstance(stats, dict) and 'avg_time' in stats:
                    if stats['avg_time'] < fastest_time:
                        fastest_time = stats['avg_time']
                        fastest_system = system
            
            if fastest_system:
                summary['best_performing'][query_name] = {
                    'system': fastest_system,
                    'time': fastest_time
                }
        
        # Calcular ratios de rendimiento
        if 'query_1_top_businesses' in comparison_results:
            query1 = comparison_results['query_1_top_businesses']
            if 'redis' in query1 and 'mongodb' in query1:
                redis_stats = query1['redis']
                mongo_stats = query1['mongodb']
                
                if isinstance(redis_stats, dict) and 'avg_time' in redis_stats and \
                   isinstance(mongo_stats, dict) and 'avg_time' in mongo_stats:
                    
                    redis_time = redis_stats['avg_time']
                    mongo_time = mongo_stats['avg_time']
                    
                    if mongo_time > 0 and redis_time > 0:
                        ratio = mongo_time / redis_time
                        summary['performance_ratios']['redis_vs_mongo'] = {
                            'ratio': ratio,
                            'description': f'Redis es {ratio:.1f}x más rápido que MongoDB para consultas simples'
                        }
        
        # Generar recomendaciones
        recommendations = [
            "Redis es ideal para caché y consultas de tiempo real",
            "MongoDB es bueno para datos semiestructurados y agregaciones",
            "Neo4j es óptimo para análisis de relaciones y grafos",
            "Considerar arquitectura híbrida según necesidades específicas"
        ]
        
        summary['recommendations'] = recommendations
        
        return summary
    
    def export_results(self, output_file: str = "benchmark_results.json"):
        """Exportar resultados a archivo JSON"""
        export_data = {
            'benchmark_timestamp': datetime.now().isoformat(),
            'results': self.results,
            'summary': self._generate_summary()
        }
        
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.info(f"Resultados exportados a {output_file}")
        return output_file
    
    def _generate_summary(self) -> Dict:
        """Generar resumen de todos los benchmarks"""
        if not self.results:
            return {}
        
        summary = {
            'total_benchmarks': len(self.results),
            'fastest_function': None,
            'slowest_function': None,
            'average_times': {}
        }
        
        fastest_time = float('inf')
        slowest_time = 0
        
        for func_name, stats in self.results.items():
            avg_time = stats['avg_time']
            
            # Actualizar más rápido/más lento
            if avg_time < fastest_time:
                fastest_time = avg_time
                summary['fastest_function'] = func_name
            
            if avg_time > slowest_time:
                slowest_time = avg_time
                summary['slowest_function'] = func_name
            
            # Agrupar por sistema
            system = func_name.split('_')[0]
            if system not in summary['average_times']:
                summary['average_times'][system] = []
            summary['average_times'][system].append(avg_time)
        
        # Calcular promedios por sistema
        for system, times in summary['average_times'].items():
            summary['average_times'][system] = {
                'avg': statistics.mean(times),
                'count': len(times)
            }
        
        return summary
    
    def visualize_results(self):
        """Visualizar resultados (requiere matplotlib)"""
        try:
            import matplotlib.pyplot as plt
            import numpy as np
            
            # Preparar datos para visualización
            systems = []
            avg_times = []
            
            for func_name, stats in self.results.items():
                system = func_name.split('_')[0]
                if system not in systems:
                    systems.append(system)
                    avg_times.append([])
                
                idx = systems.index(system)
                avg_times[idx].append(stats['avg_time'])
            
            # Crear gráfico
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
            
            # Gráfico de barras (promedio por sistema)
            system_means = [statistics.mean(times) for times in avg_times]
            bars = ax1.bar(systems, system_means)
            ax1.set_title('Tiempo promedio por sistema')
            ax1.set_ylabel('Segundos')
            ax1.set_xlabel('Sistema')
            
            # Añadir valores en las barras
            for bar, mean in zip(bars, system_means):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{mean:.4f}', ha='center', va='bottom')
            
            # Gráfico de dispersión (todas las funciones)
            all_data = []
            colors = []
            color_map = {'mongo': 'blue', 'neo4j': 'green', 'redis': 'red'}
            
            for func_name, stats in self.results.items():
                system = func_name.split('_')[0]
                all_data.append((func_name, stats['avg_time']))
                colors.append(color_map.get(system, 'gray'))
            
            func_names = [d[0] for d in all_data]
            times = [d[1] for d in all_data]
            
            ax2.scatter(range(len(func_names)), times, c=colors, alpha=0.6)
            ax2.set_title('Tiempo por función')
            ax2.set_ylabel('Segundos')
            ax2.set_xlabel('Función')
            ax2.set_xticks(range(len(func_names)))
            ax2.set_xticklabels(func_names, rotation=45, ha='right')
            
            plt.tight_layout()
            plt.savefig('benchmark_results.png', dpi=100)
            plt.show()
            
            logger.info("Gráfico guardado como benchmark_results.png")
            
        except ImportError:
            logger.warning("Matplotlib no instalado. No se puede visualizar resultados.")
            print("Instala matplotlib para visualización: pip install matplotlib")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Ejemplo de uso
    benchmark = PerformanceBenchmark()
    
    # Función de ejemplo para benchmark
    def example_function(n: int) -> int:
        """Función de ejemplo para benchmark"""
        total = 0
        for i in range(n):
            total += i
        return total
    
    # Ejecutar benchmark
    results = benchmark.benchmark_function(
        example_function, "example_function", 10000, iterations=100
    )
    
    print(f"Resultados: {results}")
    
    # Exportar resultados
    benchmark.export_results()
    
    # Generar resumen
    summary = benchmark._generate_summary()
    print(f"\nResumen: {summary}")
    
    # Intentar visualizar
    benchmark.visualize_results()