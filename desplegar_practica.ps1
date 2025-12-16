
Write-Host "=========================================" -ForegroundColor Green
Write-Host "   DESPLIEGUE PRÁCTICA UEM NoSQL" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green

# Función para verificar comandos
function Check-Command ($command, $description) {
    Write-Host "Verificando $description..." -NoNewline
    if (Get-Command $command -ErrorAction SilentlyContinue) {
        Write-Host " OK" -ForegroundColor Green
        return $true
    }
    else {
        Write-Host " NO ENCONTRADO" -ForegroundColor Red
        return $false
    }
}

# 1. Verificaciones Iniciales
Write-Host "`n[1/6] Verificando prerequisitos..." -ForegroundColor Yellow
if (-not (Check-Command "docker" "Docker")) { exit 1 }
if (-not (Check-Command "python" "Python")) { exit 1 }

if (!(docker ps)) {
    Write-Error "Docker Desktop no está ejecutándose. Por favor inícielo."
    exit 1
}

# 2. Levantar Entorno Docker
Write-Host "`n[2/6] Levantando contenedores Docker..." -ForegroundColor Yellow
docker-compose down -v 2>$null # Limpieza preventiva
docker-compose up -d

Write-Host "Esperando a que los servicios estén listos (20s)..." -ForegroundColor Cyan
Start-Sleep -Seconds 20

# Verificar servicios
$servicios = docker-compose ps --services
if ($servicios) {
    Write-Host "Servicios activos:" -ForegroundColor Green
    $servicios | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }
}
else {
    Write-Error "Fallo al levantar contenedores."
    exit 1
}

# 3. Preparar Entorno Python
Write-Host "`n[3/6] Configurando entorno Python..." -ForegroundColor Yellow
if (!(Test-Path "venv")) {
    Write-Host "Creando virtualenv..." -ForegroundColor Gray
    python -m venv venv
}

# Activar venv (manejo de errores de script execution policy)
try {
    .\venv\Scripts\Activate.ps1
}
catch {
    Write-Warning "No se pudo activar venv automáticamente. Usando python directo del venv."
}

$PYTHON = ".\venv\Scripts\python.exe"
$PIP = ".\venv\Scripts\pip.exe"

& $PIP install -r requirements.txt | Out-Null
Write-Host "Dependencias instaladas." -ForegroundColor Green

# 4. Preparación de Datos
Write-Host "`n[4/6] Procesando datos (ETL)..." -ForegroundColor Yellow
Write-Host "Generando archivos para MongoDB..." -ForegroundColor Gray
& $PYTHON scripts/data-preparation.py --mode mongo

Write-Host "Generando archivos para Neo4j..." -ForegroundColor Gray
& $PYTHON scripts/data-preparation.py --mode neo4j

# 5. Carga de Datos en Bases de Datos
Write-Host "`n[5/6] Importando datos a los contenedores..." -ForegroundColor Yellow

# MongoDB Import
Write-Host "-> MongoDB: Importando colecciones..." -ForegroundColor Cyan
docker exec uem_mongo mongoimport --username admin --password uem_password123 --authenticationDatabase admin --db yelp_mongo --collection business --file /data/import/mongo_business.json --drop
docker exec uem_mongo mongoimport --username admin --password uem_password123 --authenticationDatabase admin --db yelp_mongo --collection user --file /data/import/mongo_user.json --drop
docker exec uem_mongo mongoimport --username admin --password uem_password123 --authenticationDatabase admin --db yelp_mongo --collection review --file /data/import/mongo_review.json --drop

# Neo4j Import
Write-Host "-> Neo4j: Copiando y cargando grafos..." -ForegroundColor Cyan
docker cp data/processed/business_neo4j.csv uem_neo4j:/var/lib/neo4j/import/business_neo4j.csv
docker cp data/processed/review_neo4j.csv uem_neo4j:/var/lib/neo4j/import/review_neo4j.csv
docker cp data/processed/user_neo4j.csv uem_neo4j:/var/lib/neo4j/import/user_neo4j.csv
& $PYTHON scripts/import_neo4j.py

# 6. Ejecución y Reporte
Write-Host "`n[6/6] Ejecutando práctica y generando reporte..." -ForegroundColor Yellow
& $PYTHON main.py --mode all --report

Write-Host "`n=========================================" -ForegroundColor Green
Write-Host "✅ PRÁCTICA COMPLETADA CORRECTAMENTE" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host "`nResultados generados en:" -ForegroundColor Cyan
Write-Host "  - Informe JSON: ./results/report.json"
Write-Host "  - Resumen TXT:  ./results/summary.txt"
Write-Host "  - Logs:         ./logs/uem_practice_actividad_*.log"
Write-Host "`nAcceso a Interfaces:" -ForegroundColor Cyan
Write-Host "  - Mongo Express:   http://localhost:8081"
Write-Host "  - Neo4j Browser:   http://localhost:7474"
Write-Host "  - Redis Commander: http://localhost:8082"
