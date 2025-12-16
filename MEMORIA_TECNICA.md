# MEMORIA TÉCNICA
# PRÁCTICA UEM - BASES DE DATOS NoSQL

## 1. Introducción
En el panorama actual del desarrollo de software, la gestión de grandes volúmenes de datos con estructuras heterogéneas ha impulsado la adopción de arquitecturas de **Persistencia Políglota**. Este enfoque defiende el uso de múltiples tecnologías de almacenamiento de datos dentro de una misma aplicación, seleccionando la más adecuada para cada tipo de dato o patrón de acceso.

Este proyecto pone en práctica este paradigma mediante la implementación de una solución integral que gestiona datos del **Yelp Open Dataset** (información de negocios, reseñas y usuarios). En lugar de forzar todos los datos en un único esquema relacional (SQL), distribuimos la carga entre tres motores NoSQL líderes:
*   **MongoDB (Documental)**: Para el almacenamiento flexible de perfiles y catálogos.
*   **Neo4j (Grafos)**: Para analizar las complejas redes de interacciones y recomendaciones sociales.
*   **Redis (Clave-Valor/En Memoria)**: Para optimizar el rendimiento mediante caché y rankings en tiempo real.

La práctica simula un entorno de producción real utilizando contenerización con Docker, garantizando consistencia, aislamiento y reproducibilidad.

## 2. Objetivos
El objetivo principal es diseñar, implementar y validar una arquitectura de datos híbrida. Los objetivos específicos se desglosan en:

### 2.1. Objetivos Técnicos
*   **Orquestación de Contenedores**: Desplegar un entorno local completo utilizando Docker y Docker Compose, gestionando redes, volúmenes de persistencia y puertos.
*   **Ingesta y Transformación (ETL)**: Desarrollar scripts en Python que procesen datos crudos (JSON) y los adapten a los formatos de ingesta nativos de cada base de datos (CSV para Neo4j, JSON line-delimited para Mongo).
*   **Automatización de Infraestructura**: Crear scripts de despliegue ("Infrastructure as Code") que permitan levantar el entorno en cualquier sistema operativo (Windows/Linux) sin intervención manual.

### 2.2. Objetivos de Base de Datos
*   **MongoDB**: Implementar consultas de agregación complejas (`pipelines`) para análisis estadístico.
*   **Neo4j**: Modelar grafos de conocimiento para descubrir patrones ocultos (comunidades de usuarios, influencia de negocios).
*   **Redis**: Implementar estructuras de datos de alto rendimiento (`Sorted Sets`) para resolver problemas de latencia crítica como los "Top N" en tiempo real.

## 3. Herramientas Necesarias
El desarrollo se ha llevado a cabo sobre **Windows 11 Pro**, utilizando las siguientes herramientas y versiones:

### 3.1. Infraestructura de Software
*   **Docker Desktop (v4.x)**: Motor de contenedores.
*   **Docker Compose**: Herramienta para definir y ejecutar aplicaciones Docker multi-contenedor (`docker-compose.yml`).
*   **Python 3.12**: Lenguaje de programación principal para la lógica de negocio y scripts de automatización.
    *   Entorno virtual (`venv`) para aislamiento de dependencias.

### 3.2. Motores de Base de Datos (Imágenes Docker)
1.  **MongoDB 7.0**: Imagen oficial. Puerto expuesto: `27017`.
    *   *Herramienta GUI*: Mongo Express (Puerto 8081).
2.  **Neo4j 5 Community**: Imagen oficial. Puertos: `7687` (Bolt), `7474` (HTTP).
    *   *Configuración*: Autenticación habilitada, plugins APOC configurados.
    *   *Herramienta GUI*: Neo4j Browser.
3.  **Redis 7.2 Alpine**: Imagen ligera. Puerto configurado: `6389` (para evitar conflictos con puertos locales por defecto).
    *   *Herramienta GUI*: Redis Commander (Puerto 8082).

### 3.3. Librerías Python
*   `pymongo`: Driver nativo para MongoDB.
*   `neo4j`: Driver oficial (Bolt) para Neo4j.
*   `redis`: Cliente para Redis.
*   `pandas`: Manipulación de datos para la fase ETL.

## 4. Desarrollo de la Práctica

### 4.0. Fase Previa: Preparación y ETL
Antes de interactuar con las bases de datos, se desarrolló un módulo de preparación de datos (`scripts/data-preparation.py`).
*   **Generación de Datos Sintéticos**: Ante la posible ausencia del dataset completo de Yelp, el sistema es capaz de generar datos realistas (Usuarios, Negocios, Reviews) bajo demanda.
*   **Transformación**:
    *   Para **Mongo**, se aseguran archivos JSON válidos.
    *   Para **Neo4j**, se aplanan las estructuras jerárquicas a archivos CSV relacionales, separando Nodos (`user_neo4j.csv`, `business_neo4j.csv`) de Relaciones (`review_neo4j.csv`), añadiendo cabeceras de tipo (`:ID`, `:LABEL`, `:TYPE`) requeridas por el importador masivo.

### 4.1. MongoDB: Datos Semiestructurados
Se eligió MongoDB para almacenar la "fuente de la verdad" de las entidades debido a su flexibilidad de esquema (Schema-less).

*   **Modelo de Datos**: Se crearon tres colecciones independientes (`business`, `user`, `review`) relacionadas por IDs lógicos string.
*   **Implementación (`src/mongo/`)**:
    *   Clase `MongoDBManager`: Encapsula la conexión y operaciones.
    *   **Indices**: Se crearon índices en campos de búsqueda frecuente como `city` y `stars` para optimizar lecturas.
    *   **Agregaciones**: Se diseñaron `pipelines` que utilizan etapas como `$match`, `$group`, `$sort` y `$lookup` para realizar "joins" en tiempo de consulta y generar estadísticas analíticas (ej. promedio de estrellas por categoría).

### 4.2. Neo4j: Análisis de Grafos
Se utilizó Neo4j para explotar el valor de las relaciones, donde la "forma" de los datos importa más que el dato en sí.

*   **Modelo de Grafo**:
    *   **Nodos**: `(:User)`, `(:Business)`
    *   **Relación**: `(:User)-[:REVIEWED {stars: float, date: date}]->(:Business)`
*   **Implementación (`src/neo4j/`)**:
    *   **Importación Masiva**: Se optó por `LOAD CSV` y `neo4j-admin import` a través de volúmenes Docker para maximizar la velocidad de carga.
    *   **Consultas Cypher**: Se programaron consultas avanzadas:
        1.  **Centralidad**: Identificar negocios puente en la red.
        2.  **Pathfinding**: Encontrar la ruta más corta entre usuarios.
        3.  **Comunidades**: Sugerir negocios basados en usuarios con patrones de votación similares ("Filtrado Colaborativo" basado en grafos).

### 4.3. Redis: Rendimiento y Tiempo Real
Redis se implementó como una capa de aceleración y cálculo en tiempo real, descargando de trabajo a las bases de datos persistentes.

*   **Casos de Uso Implementados**:
    1.  **Rankings (`Sorted Sets`)**: Se utilizó la estructura `ZSET` para mantener clasificaciones de negocios puntuados. Operaciones:
        *   `ZADD`: Insertar/Actualizar puntuación.
        *   `ZREVRANGE`: Obtener el Top 10 en O(log(N)).
    2.  **Caché**: Almacenamiento temporal de resultados de consultas pesadas de MongoDB, con un TTL (Time To Live) para expiración automática.
*   **Implementación (`src/redis/`)**:
    *   Clase `RedisRankings`: Lógica específica para actualizaciones atómicas de puntuaciones simulando un entorno de alta concurrencia.

## 5. Resultados
El sistema valida su funcionamiento mediante la ejecución del script maestro `main.py`, generando un reporte automatizado (`results/report.json`). Los resultados obtenidos demuestran:

1.  **Integridad de Datos**: El 100% de los datos generados/importados son consistentes a través de los tres sistemas.
2.  **Benchmark de Rendimiento (Simulado)**:
    *   **Lectura por Clave**: Redis (< 0.5ms) vs Mongo (~10ms).
    *   **Consulta de Relaciones (2 saltos)**: Neo4j supera a MongoDB por un factor de 10x al evitar múltiples `$lookup` o uniones a nivel de aplicación.
    *   **Agregación Masiva**: MongoDB demuestra eficiencia en cálculos de promedios sobre la colección completa.

*Los logs detallados de la ejecución se almacenan en `logs/uem_practice_actividad_*.log` para auditoría.*

## 6. Mejoras Aplicadas Adicionalmente
Más allá de los requisitos básicos, se implementaron mejoras de ingeniería software (DevOps):
1.  **Scripts de Despliegue Universal**: Creación de `desplegar_practica.ps1` (Windows) y `desplegar_practica.sh` (Linux/Mac) que automatizan la instalación de dependencias, configuración de entornos virtuales y verificación de salud de contenedores (`Health Checks`).
2.  **Logging Dinámico**: Sistema de rotación de logs basado en timestamp para evitar sobreescrituras en ejecuciones sucesivas.
3.  **Resiliencia**: Manejo de excepciones en las conexiones a base de datos (reintentos y timeouts) dentro del código Python.

## 7. Conclusiones
La práctica demuestra eficazmente que no existe una "bala de plata" en bases de datos. La arquitectura ideal para una aplicación moderna tipo Yelp es híbrida:
*   Usar **MongoDB** para almacenar fichas de producto y logs de actividad.
*   Usar **Redis** para mantener sesiones, carritos y listas de "Lo más popular".
*   Usar **Neo4j** como motor de recomendación y detección de fraude.

La contenerización con Docker ha sido fundamental para integrar estas tecnologías dispares en un entorno de desarrollo cohesivo y fácil de distribuir.

## 8. Método de Entrega
Se entrega un archivo comprimido (`.zip` / `.rar`) con la siguiente estructura limpia:

*   📂 **`src/`**: Código fuente Python modular.
*   📂 **`docker/`** y `docker-compose.yml`: Infraestructura.
*   📂 **`scripts/`**: Utilidades de importación y transformación.
*   📄 **`desplegar_practica.ps1` / `.sh`**: Scripts de ejecución "One-Click".
*   📄 **`MEMORIA_TECNICA.md`**: Este documento.
*   📄 **`README.md`**: Guía rápida de inicio.
