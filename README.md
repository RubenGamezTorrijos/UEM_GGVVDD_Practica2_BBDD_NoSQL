# UEM Práctica 2 - Bases de Datos NoSQL en Docker

Este proyecto implementa una arquitectura políglota para comparar y utilizar las fortalezas de **MongoDB** (Documental), **Neo4j** (Grafos) y **Redis** (Clave-Valor/Caché) sobre el **Yelp Open Dataset**.

## 📋 Prerequisitos
*   **Docker Desktop 4.55.0** instalado y en ejecución.
*   **Python 3.10** o superior instalado.
*   Conexión a internet (para descargar imágenes Docker y librerías).

## 🚀 Despliegue Rápido
El proyecto incluye scripts automatizados que levantan el entorno, cargan los datos y ejecutan la práctica completa.

### En Windows
Abrir PowerShell como Administrador y ejecutar:
```powershell
.\desplegar_practica.ps1
```

### En Linux / MacOS
Dar permisos de ejecución y lanzar el script:
```bash
chmod +x desplegar_practica.sh
./desplegar_practica.sh
```

---

## 🔍 ¿Qué hace el script de despliegue?
1.  Verifica que Docker y Python estén instalados.
2.  Levanta los contenedores (Mongo, Neo4j, Redis y sus interfaces web).
3.  Crea un entorno virtual Python (`venv`) e instala las dependencias.
4.  Genera datos sintéticos y los importa en las bases de datos.
5.  Ejecuta el análisis comparativo (`main.py`).
6.  Genera un reporte final en la carpeta `results/`.

## 🌐 Acceso a Interfaces Web
Una vez desplegado, puedes explorar los datos visualmente en:
*   **Mongo Express**: [http://localhost:8081](http://localhost:8081)
    *   *Usuario*: admin / *Pass*: uem_password123
*   **Neo4j Browser**: [http://localhost:7474](http://localhost:7474)
    *   *Conexión*: bolt://localhost:7687
    *   *Usuario*: neo4j / *Pass*: uem_password123
*   **Redis Commander**: [http://localhost:8082](http://localhost:8082)

## 📂 Resultados y Logs
*   **Informe Final**: `results/report.json` (Datos completos del benchmark).
*   **Resumen Ejecutivo**: `results/summary.txt` (Conclusiones principales).
*   **Logs**: `logs/uem_practice_actividad_*.log` (Registro detallado por fecha de ejecución).

## 🛠️ Estructura del Proyecto
*   `src/`: Código fuente de los gestores de base de datos.
*   `access/`: Scripts y datos para la carga inicial.
*   `docker-compose.yml`: Definición de la infraestructura.
*   `MEMORIA_TECNICA.md`: Documentación detallada del desarrollo y arquitectura.

---
**Autor**: Ruben Gamez Torrijos

**Versión**: 1.0.0
