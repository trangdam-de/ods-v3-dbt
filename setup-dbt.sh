#!/bin/bash

# DBT Setup and Test Script for ODS ETL System
# This script initializes DBT and runs basic tests

set -e

echo "🚀 Setting up DBT for ODS ETL System..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to run dbt commands in container
run_dbt_command() {
    local command=$1
    echo -e "${BLUE}Running: dbt ${command}${NC}"
    docker exec -it $(docker ps -q --filter label=com.docker.swarm.service.name=airflow_dbt) \
        bash -c "cd /usr/app/dbt && dbt ${command}"
}

# Check if DBT container is running
echo -e "${YELLOW}📋 Checking DBT container status...${NC}"
if ! docker ps --filter label=com.docker.swarm.service.name=airflow_dbt | grep -q airflow_dbt; then
    echo -e "${RED}❌ DBT container is not running. Please start the stack first.${NC}"
    echo "Run: docker stack deploy -c docker-compose-swarm.yml airflow"
    exit 1
fi

echo -e "${GREEN}✅ DBT container is running${NC}"

# Install DBT packages
echo -e "${YELLOW}📦 Installing DBT packages...${NC}"
run_dbt_command "deps"

# Test database connection
echo -e "${YELLOW}🔌 Testing database connection...${NC}"
run_dbt_command "debug"

# Parse and compile models
echo -e "${YELLOW}🔧 Parsing and compiling DBT models...${NC}"
run_dbt_command "parse"
run_dbt_command "compile"

# Run staging models first
echo -e "${YELLOW}🏗️  Running staging models...${NC}"
run_dbt_command "run --select staging"

# Run tests on staging models
echo -e "${YELLOW}🧪 Running tests on staging models...${NC}"
run_dbt_command "test --select staging"

# Run marts models
echo -e "${YELLOW}🏭 Running marts models...${NC}"
run_dbt_command "run --select marts"

# Run all tests
echo -e "${YELLOW}🧪 Running all tests...${NC}"
run_dbt_command "test"

# Generate documentation
echo -e "${YELLOW}📚 Generating documentation...${NC}"
run_dbt_command "docs generate"

echo -e "${GREEN}🎉 DBT setup completed successfully!${NC}"
echo -e "${BLUE}📋 Summary:${NC}"
echo -e "  • DBT container: ✅ Running"
echo -e "  • Packages: ✅ Installed"
echo -e "  • Database connection: ✅ Working"
echo -e "  • Models: ✅ Compiled and executed"
echo -e "  • Tests: ✅ Passed"
echo -e "  • Documentation: ✅ Generated"

echo -e "${YELLOW}📖 Next steps:${NC}"
echo -e "  1. Access DBT docs: Run 'dbt docs serve' in container"
echo -e "  2. View logs: Check /mnt/airflow/logs/dbt/"
echo -e "  3. Customize models in /mnt/airflow/dbt/models/"
echo -e "  4. Update Airflow DAGs to use DbtRunOperator"

echo -e "${GREEN}🚀 DBT is ready for ODS ETL pipeline!${NC}"
