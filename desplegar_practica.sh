#!/bin/bash

# Colores y Formato
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}   DESPLIEGUE PRÁCTICA UEM NoSQL${NC}"
echo -e "${GREEN}=========================================${NC}"

# Función de verificación
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}Error: $1 no está instalado.${NC}"
        exit 1
    fi
}

# 1. Verificaciones
echo -e "\n${YELLOW}[1/6] Verificando prerequisitos...${NC}"
check_command docker
check_command python3
check_command pip3

if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker no está ejecutándose.${NC}"
    exit 1
fi

# 2. Levantar Docker
echo -e "\n${YELLOW}[2/6] Levantando contenedores Docker...${NC}"
docker-compose down -v 2>/dev/null
docker-compose up -d

echo -e "${CYAN}Esperando a que los servicios estén listos (20s)...${NC}"
sleep 20

if [ "$(docker-compose ps -q | wc -l)" -lt 3 ]; then
    echo -e "${RED}Error: Algunos contenedores no arrancaron.${NC}"
    exit 1
fi

# 3. Entorno Python
echo -e "\n${YELLOW}[3/6] Configurando entorno Python...${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "Virtualenv creado."
fi

source venv/bin/activate
pip install -r requirements.txt > /dev/null 2>&1
echo -e "${GREEN}Dependencias instaladas.${NC}"

# 4. Datos
echo -e "\n${YELLOW}[4/6] Procesando datos (ETL)...${NC}"
python3 scripts/data-preparation.py --mode mongo
python3 scripts/data-preparation.py --mode neo4j

# 5. Importación
echo -e "\n${YELLOW}[5/6] Importando datos a los contenedores...${NC}"

# Mongo
echo -e "${CYAN}-> MongoDB: Importando colecciones...${NC}"
docker exec uem_mongo mongoimport --username admin --password uem_password123 --authenticationDatabase admin --db yelp_mongo --collection business --file /data/import/mongo_business.json --drop
docker exec uem_mongo mongoimport --username admin --password uem_password123 --authenticationDatabase admin --db yelp_mongo --collection user --file /data/import/mongo_user.json --drop
docker exec uem_mongo mongoimport --username admin --password uem_password123 --authenticationDatabase admin --db yelp_mongo --collection review --file /data/import/mongo_review.json --drop

# Neo4j
echo -e "${CYAN}-> Neo4j: Copiando y cargando grafos...${NC}"
docker cp data/processed/business_neo4j.csv uem_neo4j:/var/lib/neo4j/import/business_neo4j.csv
docker cp data/processed/review_neo4j.csv uem_neo4j:/var/lib/neo4j/import/review_neo4j.csv
docker cp data/processed/user_neo4j.csv uem_neo4j:/var/lib/neo4j/import/user_neo4j.csv
python3 scripts/import_neo4j.py

# 6. Ejecución
echo -e "\n${YELLOW}[6/6] Ejecutando práctica y generando reporte...${NC}"
python3 main.py --mode all --report

echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}✅ PRÁCTICA COMPETADA EXITOSAMENTE${NC}"
echo -e "${GREEN}=========================================${NC}"
echo -e "\nResultados generados en:"
echo -e "  - Informe JSON: ./results/report.json"
echo -e "  - Logs:         ./logs/uem_practice_actividad_*.log"
echo -e "\nAcceso a Interfaces:"
echo -e "  - Mongo Express:   http://localhost:8081"
echo -e "  - Neo4j Browser:   http://localhost:7474"
echo -e "  - Redis Commander: http://localhost:8082"
