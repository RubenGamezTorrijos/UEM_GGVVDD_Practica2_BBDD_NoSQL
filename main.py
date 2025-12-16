#!/usr/bin/env python3
# main.py - Script principal de la practica

import argparse
import logging
from pathlib import Path
import json
from datetime import datetime

from src.mongo.database import MongoDBManager
from src.neo4j.database import Neo4jManager
from src.redis.database import RedisManager
from src.utils.performance import PerformanceBenchmark

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/uem_practice_actividad_{datetime.now().strftime('%Y%b%d_%H%M').upper()}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NoSQLPractice:
    def __init__(self):
        self.mongo = None
        self.neo4j = None
        self.redis = None
        self.benchmark = PerformanceBenchmark()
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'author': os.getenv('AUTHOR', 'Ruben Gamez Torrijos'),
            'systems': {}
        }
    
    def setup_databases(self):
        """Configurar todas las bases de datos"""
        logger.info("Inicializando sistemas NoSQL...")
        
        try:
            self.mongo = MongoDBManager()
            self.neo4j = Neo4jManager()
            try:
                self.redis = RedisManager()
            except Exception as e:
                logger.warning(f"No se pudo inicializar Redis: {e}")
                self.redis = None
            
            logger.info("Sistemas NoSQL inicializados (Redis opcional)")
            return True
            
        except Exception as e:
            logger.error(f"Error al inicializar sistemas: {e}")
            return False
    
    def run_mongo_section(self):
        """Ejecutar secciÃ³n de MongoDB"""
        logger.info("="*60)
        logger.info("EJECUTANDO SECCIÃ“N MONGODB")
        logger.info("="*60)
        
        section_results = {}
        
        # 1. Crear Ã­ndices (con drop_existing=True para limpiar)
        try:
            indexes_created = self.mongo.create_indexes(drop_existing=True)
            section_results['indexes_created'] = indexes_created
        except Exception as e:
            logger.error(f"Error creando Ã­ndices: {e}")
            section_results['indexes_created'] = 0
        
        # 2. Insertar datos de muestra si no hay datos
        business_count = self.mongo.db.business.count_documents({})
        if business_count == 0:
            logger.info("Insertando datos de muestra en MongoDB...")
            self._insert_sample_data_mongo()
            business_count = self.mongo.db.business.count_documents({})
        if business_count == 0:
            logger.info("Insertando datos de muestra en MongoDB...")
            self._insert_sample_data_mongo()
            business_count = self.mongo.db.business.count_documents({})
        if business_count == 0:
            logger.info("Insertando datos de muestra en MongoDB...")
            self._insert_sample_data_mongo()
            business_count = self.mongo.db.business.count_documents({})
        
        # 3. Ejecutar consultas de agregaciÃ³n si hay datos
        try:
            if business_count > 0:
                aggregation_results = self.mongo.run_aggregation_queries()
                section_results['aggregations'] = {
                    'count': len(aggregation_results),
                    'execution_times': {k: v['time'] for k, v in aggregation_results.items()}
                }
            else:
                logger.warning("No hay datos en MongoDB para ejecutar agregaciones")
                section_results['aggregations'] = {'count': 0, 'message': 'No hay datos'}
        except Exception as e:
            logger.error(f"Error ejecutando agregaciones: {e}")
            section_results['aggregations'] = {'error': str(e), 'count': 0}
        
        # 4. Benchmark de Ã­ndices (solo si hay datos)
        try:
            if business_count > 0:
                index_benchmark = self.mongo.benchmark_index_performance()
                section_results['index_benchmark'] = index_benchmark
            else:
                section_results['index_benchmark'] = {'message': 'No hay datos para benchmark'}
        except Exception as e:
            logger.error(f"Error en benchmark de Ã­ndices: {e}")
            section_results['index_benchmark'] = {'error': str(e)}
        
        self.results['systems']['mongodb'] = section_results
        return section_results
    
    def _insert_sample_data_mongo(self):
        """Insertar datos de muestra en MongoDB"""
        try:
            # Datos de muestra para negocios
            sample_businesses = []
            for i in range(100):
                business = {
                    "business_id": f"uem_sample_{i}",
                    "name": f"Sample Business {i}",
                    "city": f"City_{i % 10}",
                    "state": "SP",
                    "stars": 3.0 + (i % 5 * 0.5),
                    "review_count": 10 + (i * 5),
                    "categories": ["Restaurants", "Food"] if i % 2 == 0 else ["Shopping", "Retail"],
                    "is_open": 1 if i % 3 != 0 else 0
                }
                sample_businesses.append(business)
            
            # Datos de muestra para usuarios
            sample_users = []
            for i in range(50):
                user = {
                    "user_id": f"uem_user_{i}",
                    "name": f"User {i}",
                    "review_count": 5 + (i * 2),
                    "yelping_since": "2018-01-01",
                    "useful": i * 3,
                    "funny": i * 2,
                    "cool": i * 4
                }
                sample_users.append(user)
            
            # Datos de muestra para reseÃ±as
            sample_reviews = []
            for i in range(200):
                review = {
                    "review_id": f"uem_review_{i}",
                    "user_id": f"uem_user_{i % 50}",
                    "business_id": f"uem_sample_{i % 100}",
                    "stars": 1 + (i % 5),
                    "date": "2023-01-01",
                    "text": f"Sample review text {i}",
                    "useful": i % 10,
                    "funny": i % 5,
                    "cool": i % 7
                }
                sample_reviews.append(review)
            
            # Insertar datos
            self.mongo.db.business.insert_many(sample_businesses)
            self.mongo.db.user.insert_many(sample_users)
            self.mongo.db.review.insert_many(sample_reviews)
            
            logger.info(f"Datos de muestra insertados: {len(sample_businesses)} negocios, "
                       f"{len(sample_users)} usuarios, {len(sample_reviews)} reseÃ±as")
            
        except Exception as e:
            logger.error(f"Error insertando datos de muestra: {e}")


        
    def _insert_sample_data_mongo(self):
        """Insertar datos de muestra en MongoDB"""
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            # Datos de muestra para negocios
            sample_businesses = []
            for i in range(100):
                business = {
                    "business_id": f"uem_sample_{i}",
                    "name": f"Sample Business {i}",
                    "city": f"City_{i % 10}",
                    "state": "SP",
                    "stars": 3.0 + (i % 5 * 0.5),
                    "review_count": 10 + (i * 5),
                    "categories": ["Restaurants", "Food"] if i % 2 == 0 else ["Shopping", "Retail"],
                    "is_open": 1 if i % 3 != 0 else 0
                }
                sample_businesses.append(business)
            
            # Datos de muestra para usuarios
            sample_users = []
            for i in range(50):
                user = {
                    "user_id": f"uem_user_{i}",
                    "name": f"User {i}",
                    "review_count": 5 + (i * 2),
                    "yelping_since": "2018-01-01",
                    "useful": i * 3,
                    "funny": i * 2,
                    "cool": i * 4
                }
                sample_users.append(user)
            
            # Insertar datos
            self.mongo.db.business.insert_many(sample_businesses)
            self.mongo.db.user.insert_many(sample_users)
            
            logger.info(f"Datos de muestra insertados: {len(sample_businesses)} negocios, "
                       f"{len(sample_users)} usuarios")
            
        except Exception as e:
            logger.error(f"Error insertando datos de muestra: {e}")

    def run_neo4j_section(self):
        """Ejecutar secciÃ³n de Neo4j"""
        logger.info("="*60)
        logger.info("EJECUTANDO SECCION NEO4J")
        logger.info("="*60)
        
        section_results = {}
        
        # 1. Ejecutar consultas Cypher
        cypher_results = self.neo4j.run_cypher_queries()
        section_results['cypher_queries'] = {
            'count': len(cypher_results),
            'sample_results': {k: v[:2] for k, v in cypher_results.items() if v}
        }
        
        # 2. Analizar patrones del grafo
        graph_patterns = self.neo4j.visualize_graph_patterns()
        section_results['graph_patterns'] = graph_patterns
        
        self.results['systems']['neo4j'] = section_results
        return section_results
    
    def run_redis_section(self):
        """Ejecutar seccion de Redis"""
        logger.info("="*60)
        logger.info("EJECUTANDO SECCION REDIS")
        logger.info("="*60)
        
        section_results = {}
        
        # 1. Cargar datos de ejemplo desde los datos originales de Yelp
        sample_path = Path(__file__).parent / "data" / "raw" / "business.json"
        
        # Verificar si existe
        if not sample_path.exists():
             logger.error(f"No encontrado {sample_path}. Descargue el Yelp Dataset primero.")
             return {'error': 'Dataset not found'}
        
        with open(sample_path, 'r', encoding='utf-8') as f:
            # Leer solo los primeros 100 negocios para el ranking de ejemplo
            sample_data = []
            for i, line in enumerate(f):
                if i >= 100: break
                try:
                    sample_data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        
        # 2. Crear rankings
        self.redis.create_rankings(sample_data)
        section_results['rankings_created'] = len(sample_data)
        
        # 3. Benchmark de rendimiento
        redis_benchmark = self.redis.benchmark_performance()
        section_results['benchmark'] = redis_benchmark
        
        # 4. CaracterÃ­sticas en tiempo real
        realtime_features = self.redis.implement_real_time_features()
        section_results['realtime_features'] = realtime_features
        
        self.results['systems']['redis'] = section_results
        return section_results
    
    def run_comparative_analysis(self):
        """Ejecutar anÃ¡lisis comparativo"""
        logger.info("="*60)
        logger.info("ANÃLISIS COMPARATIVO")
        logger.info("="*60)
        
        comparative_results = {}
        
        # 1. Comparativa de rendimiento
        performance_data = self.benchmark.compare_systems(
            self.mongo, self.neo4j, self.redis
        )
        comparative_results['performance'] = performance_data
        
        # 2. AnÃ¡lisis de casos de uso
        use_case_analysis = {
            'mongodb': {
                'best_for': ['Datos semiestructurados', 'Agregaciones complejas', 'Rapid prototyping'],
                'performance': 'Alto rendimiento en lecturas/escrituras',
                'complexity': 'Media - FÃ¡cil de empezar, complejo en escala'
            },
            'neo4j': {
                'best_for': ['Relaciones complejas', 'Path finding', 'Sistemas de recomendaciÃ³n'],
                'performance': 'Optimo para consultas de grafos',
                'complexity': 'Alta - Modelado de grafos requiere planificaciÃ³n'
            },
            'redis': {
                'best_for': ['CachÃ©', 'Tiempo real', 'Sessions', 'Rankings dinÃ¡micos'],
                'performance': 'Extremo - Microsegundos de latencia',
                'complexity': 'Baja/Media - Estructuras de datos simples'
            }
        }
        comparative_results['use_cases'] = use_case_analysis
        
        # 3. Conclusiones
        conclusions = {
            'data_modeling': 'Cada sistema optimiza un patrÃ³n diferente de datos',
            'performance_tradeoffs': 'Redis > MongoDB > Neo4j para consultas simples',
            'recommendations': 'Sistemas hi­bridos ofrecen el mejor balance',
            'learning_curve': 'MongoDB mas accesible, Neo4j mas especializado'
        }
        comparative_results['conclusions'] = conclusions
        
        self.results['comparative_analysis'] = comparative_results
        return comparative_results
    
    def generate_report(self, output_path="results/report.json"):
        """Generar reporte completo"""
        logger.info("Generando reporte completo...")
        
        # Crear directorio si no existe
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Guardar resultados en JSON
        with open(output_path, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        # Generar resumen ejecutivo
        summary = self._generate_summary()
        summary_path = Path(output_path).parent / "summary.txt"
        with open(summary_path, 'w') as f:
            f.write(summary)
        
        logger.info(f"Reporte guardado en: {output_path}")
        logger.info(f"Resumen guardado en: {summary_path}")
        
        return output_path
    
    def _generate_summary(self):
        """Generar resumen ejecutivo"""
        from datetime import datetime
        summary_lines = [
            "="*60,
            "PRÁCTICA UEM - BASES DE DATOS NoSQL",
            "="*60,
            f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Autor: {self.results.get('author', 'Tu_Nombre')}",
            "",
            "RESULTADOS PRINCIPALES:",
            ""
        ]
        
        # MongoDB
        if 'mongodb' in self.results.get('systems', {}):
            mongo_data = self.results['systems']['mongodb']
            summary_lines.extend([
                "MONGODB:",
                f"- Índices creados: {mongo_data.get('indexes_created', 0)}",
                f"- Consultas ejecutadas: {mongo_data.get('aggregations', {}).get('count', 0)}"
            ])
            
            # Agregar mejora de índices si existe
            index_benchmark = mongo_data.get('index_benchmark', {})
            if index_benchmark and isinstance(index_benchmark, dict):
                # Buscar cualquier valor de mejora en el benchmark
                for key, value in index_benchmark.items():
                    if isinstance(value, dict) and 'improvement' in value:
                        summary_lines.append(f"- Mejora con índices: {value['improvement']}")
                        break
                    elif isinstance(value, str) and 'improvement' in value:
                        summary_lines.append(f"- Mejora con índices: {value}")
                        break
            
            summary_lines.append("")
        
        # Neo4j
        if 'neo4j' in self.results.get('systems', {}):
            neo4j_data = self.results['systems']['neo4j']
            cypher_queries = neo4j_data.get('cypher_queries', {})
            summary_lines.extend([
                "NEO4J:",
                f"- Consultas Cypher: {cypher_queries.get('count', 0)}"
            ])
            
            # Agregar densidad del grafo si existe
            graph_patterns = neo4j_data.get('graph_patterns', {})
            if graph_patterns and 'graph_density' in graph_patterns:
                density = graph_patterns['graph_density'].get('connection_density', 0)
                summary_lines.append(f"- Densidad del grafo: {density:.6f}")
            
            summary_lines.append("")
        
        # Redis
        if 'redis' in self.results.get('systems', {}):
            redis_data = self.results['systems']['redis']
            summary_lines.extend([
                "REDIS:",
                f"- Rankings creados: {redis_data.get('rankings_created', 0)}"
            ])
            
            # Agregar mejora con caché si existe
            benchmark = redis_data.get('benchmark', {})
            if benchmark and 'city_search' in benchmark:
                city_search = benchmark['city_search']
                if isinstance(city_search, dict) and 'improvement' in city_search:
                    summary_lines.append(f"- Mejora con caché: {city_search['improvement']}")
            
            summary_lines.append("")
        
        # Análisis comparativo si existe
        if 'comparative_analysis' in self.results:
            comp_data = self.results['comparative_analysis']
            summary_lines.extend([
                "ANÁLISIS COMPARATIVO:",
                "- MongoDB: Ideal para datos semiestructurados y agregaciones",
                "- Neo4j: Óptimo para relaciones complejas y análisis de grafos",
                "- Redis: Superior para tiempo real y caché de alto rendimiento",
                ""
            ])
        
        # Conclusiones y recomendación
        summary_lines.extend([
            "CONCLUSIONES:",
            "Cada sistema NoSQL optimiza un patrón diferente de datos:",
            "   MongoDB  Flexibilidad en esquemas y agregaciones",
            "   Neo4j  Relaciones complejas y análisis de grafos", 
            "   Redis  Rendimiento extremo y operaciones en tiempo real",
            "",
            "RECOMENDACIÓN:",
            "Sistema híbrido usando:",
            "   Redis para caché y rankings en tiempo real",
            "   MongoDB para datos principales y agregaciones",
            "   Neo4j para análisis de relaciones y recomendaciones",
            "="*60
        ])
        
        return "\n".join(summary_lines)
    
    def cleanup(self):
        """Limpieza de recursos"""
        logger.info("Realizando limpieza de recursos...")
        
        if self.mongo:
            self.mongo.close()
        if self.neo4j:
            self.neo4j.close()
        if self.redis:
            self.redis.close()
        
        logger.info("Limpieza completada")

def main():
    """FunciÃ³n principal"""
    parser = argparse.ArgumentParser(description='PrÃ¡ctica UEM - Bases de Datos NoSQL')
    parser.add_argument('--mode', choices=['all', 'mongo', 'neo4j', 'redis', 'compare'],
                       default='all', help='Modo de ejecuciÃ³n')
    parser.add_argument('--report', action='store_true',
                       help='Generar reporte al finalizar')
    
    args = parser.parse_args()
    
    practice = NoSQLPractice()
    
    try:
        # Configurar bases de datos
        if not practice.setup_databases():
            logger.error("No se pudieron inicializar todos los sistemas")
            return
        
        # Ejecutar segÃºn modo seleccionado
        if args.mode in ['all', 'mongo']:
            practice.run_mongo_section()
        
        if args.mode in ['all', 'neo4j']:
            practice.run_neo4j_section()
        
        if args.mode in ['all', 'redis'] and practice.redis:
            practice.run_redis_section()
        
        if args.mode in ['all', 'compare']:
            practice.run_comparative_analysis()
        
        # Generar reporte si se solicita
        if args.report or args.mode == 'all':
            report_path = practice.generate_report()
            print(f"\nReporte generado en: {report_path}")
            
            # Mostrar resumen en consola
            print("\n" + "="*60)
            print("RESUMEN EJECUTIVO")
            print("="*60)
            print(practice._generate_summary())
        
        logger.info("PrÃ¡ctica completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error durante la ejecucion: {e}")
        raise
    
    finally:
        practice.cleanup()

if __name__ == "__main__":
    import os
    main()
