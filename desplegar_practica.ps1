
Write-Host "=========================================" -ForegroundColor Green
Write-Host "   DESPLIEGUE PRÁCTICA UEM NoSQL" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green

# Función para verificar comandos
function Test-Command ($command, $description) {
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
Write-Host "`n[1/7] Verificando prerequisitos..." -ForegroundColor Yellow
if (-not (Test-Command "docker" "Docker")) { exit 1 }
if (-not (Test-Command "python" "Python")) { exit 1 }

if (!(docker ps)) {
    Write-Error "Docker Desktop no está ejecutándose. Por favor inícielo."
    exit 1
}

# 2. Levantar Entorno Docker
Write-Host "`n[2/7] Levantando contenedores Docker..." -ForegroundColor Yellow
docker-compose down -v 2>$null # Limpieza preventiva
docker-compose up -d

Write-Host "Esperando a que los servicios estén listos (25s)..." -ForegroundColor Cyan
Start-Sleep -Seconds 25

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
Write-Host "`n[3/7] Configurando entorno Python..." -ForegroundColor Yellow
if (!(Test-Path "venv")) {
    Write-Host "Creando virtualenv..." -ForegroundColor Gray
    python -m venv venv
}

$PYTHON = ".\venv\Scripts\python.exe"
$PIP = ".\venv\Scripts\pip.exe"

& $PIP install -r requirements.txt --quiet
Write-Host "Dependencias instaladas." -ForegroundColor Green

# 4. Preparación de Datos
Write-Host "`n[4/7] Procesando datos (ETL)..." -ForegroundColor Yellow

# Verificar que existen los datos raw de Yelp
if (!(Test-Path "data\raw\business.json")) {
    Write-Warning "No se encontraron datos de Yelp en data/raw/"
    Write-Host "Generando datos sintéticos para demostración..." -ForegroundColor Cyan
    & $PYTHON scripts/data-preparation.py --mode mongo
    & $PYTHON scripts/data-preparation.py --mode neo4j
}
else {
    Write-Host "Datos de Yelp encontrados. Preparando para importación..." -ForegroundColor Gray
    & $PYTHON scripts/data-preparation.py --mode mongo
    & $PYTHON scripts/data-preparation.py --mode neo4j
}

# 5. Importación a MongoDB
Write-Host "`n[5/7] Importando datos a MongoDB..." -ForegroundColor Yellow

# Usar los archivos procesados que están en formato correcto
Write-Host "-> Importando colección 'business'..." -ForegroundColor Cyan
docker exec uem_mongo mongoimport `
    --username admin `
    --password uem_password123 `
    --authenticationDatabase admin `
    --db yelp_mongo `
    --collection business `
    --file /data/processed/mongo_business.json `
    --drop

Write-Host "-> Importando colección 'user'..." -ForegroundColor Cyan
docker exec uem_mongo mongoimport `
    --username admin `
    --password uem_password123 `
    --authenticationDatabase admin `
    --db yelp_mongo `
    --collection user `
    --file /data/processed/mongo_user.json `
    --drop

Write-Host "-> Importando colección 'review'..." -ForegroundColor Cyan
docker exec uem_mongo mongoimport `
    --username admin `
    --password uem_password123 `
    --authenticationDatabase admin `
    --db yelp_mongo `
    --collection review `
    --file /data/processed/mongo_review.json `
    --drop

# 6. Importación a Neo4j
Write-Host "`n[6/7] Importando datos a Neo4j..." -ForegroundColor Yellow

Write-Host "-> Copiando archivos CSV al contenedor..." -ForegroundColor Cyan
docker cp data/processed/business_neo4j.csv uem_neo4j:/var/lib/neo4j/import/business_neo4j.csv
docker cp data/processed/review_neo4j.csv uem_neo4j:/var/lib/neo4j/import/review_neo4j.csv
docker cp data/processed/user_neo4j.csv uem_neo4j:/var/lib/neo4j/import/user_neo4j.csv

Write-Host "-> Ejecutando importación con Cypher..." -ForegroundColor Cyan
& $PYTHON scripts/import_neo4j.py

# 7. Ejecución y Reporte
Write-Host "`n[7/7] Ejecutando práctica y generando reporte..." -ForegroundColor Yellow
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
