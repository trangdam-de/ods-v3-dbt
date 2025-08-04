#!/bin/bash

# Airflow Code Sync Script
# Sync code t? development folder t?i NFS shared storage

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Paths
LOCAL_DEV="/home/dmst_doisoatods/ods/airflow"
NFS_SHARED="/srv/nfs/airflow"
MOUNT_POINT="/mnt/airflow"

echo -e "${BLUE}?? Airflow Code Sync Script${NC}"
echo -e "Source: ${GREEN}${LOCAL_DEV}${NC}"
echo -e "Target: ${GREEN}${NFS_SHARED}${NC}"
echo ""

# Function to sync specific directories
sync_directories() {
    local dirs=("dags" "plugins" "helper1" "configs" "soda" "pipeline_params" "env")
    
    for dir in "${dirs[@]}"; do
        if [ -d "${LOCAL_DEV}/${dir}" ]; then
            echo -e "${YELLOW}?? Syncing ${dir}/...${NC}"
            # Use --no-times to avoid NFS timestamp permission warnings
            rsync -av --delete --no-times --no-perms "${LOCAL_DEV}/${dir}/" "${NFS_SHARED}/${dir}/" 2>/dev/null || {
                # Fallback without delete if rsync fails
                rsync -av --no-times --no-perms "${LOCAL_DEV}/${dir}/" "${NFS_SHARED}/${dir}/" 2>/dev/null
            }
            echo -e "${GREEN}? ${dir}/ synced${NC}"
        else
            echo -e "${YELLOW}??  ${dir}/ not found, skipping${NC}"
        fi
    done
}

# Function to sync specific files
sync_files() {
    local files=("docker-compose*.yml" "*.sh" "*.md" ".env")
    
    echo -e "${YELLOW}?? Syncing configuration files...${NC}"
    for pattern in "${files[@]}"; do
        if ls "${LOCAL_DEV}"/${pattern} 1> /dev/null 2>&1; then
            cp "${LOCAL_DEV}"/${pattern} "${NFS_SHARED}/" 2>/dev/null || true
        fi
    done
    echo -e "${GREEN}? Configuration files synced${NC}"
}

# Function to set proper permissions
fix_permissions() {
    echo -e "${YELLOW}?? Fixing permissions...${NC}"
    sudo chown -R nobody:nogroup "${NFS_SHARED}"
    sudo chmod -R 755 "${NFS_SHARED}"
    sudo chmod -R 777 "${NFS_SHARED}/logs" 2>/dev/null || true
    sudo chmod -R 777 "${NFS_SHARED}/temp" 2>/dev/null || true
    echo -e "${GREEN}? Permissions fixed${NC}"
}

# Function to restart scheduler to reload DAGs
restart_scheduler() {
    echo -e "${YELLOW}?? Restarting Airflow scheduler to reload DAGs...${NC}"
    docker service update --force airflow_airflow-scheduler > /dev/null 2>&1 || true
    echo -e "${GREEN}? Scheduler restart initiated${NC}"
}

# Function to show sync status
show_status() {
    echo ""
    echo -e "${BLUE}?? Sync Status:${NC}"
    echo -e "Last sync: $(date)"
    echo -e "Files in NFS: $(find ${MOUNT_POINT} -type f | wc -l) files"
    echo -e "DAGs count: $(find ${MOUNT_POINT}/dags -name "*.py" 2>/dev/null | wc -l) Python files"
    echo ""
    echo -e "${GREEN}?? Code sync completed!${NC}"
    echo -e "${YELLOW}?? Tip: DAGs will be automatically reloaded by scheduler in ~30 seconds${NC}"
}

# Parse command line arguments
case "${1:-full}" in
    "dags")
        echo -e "${YELLOW}?? Syncing DAGs only...${NC}"
        rsync -av --delete --no-times --no-perms "${LOCAL_DEV}/dags/" "${NFS_SHARED}/dags/" 2>/dev/null || {
            rsync -av --no-times --no-perms "${LOCAL_DEV}/dags/" "${NFS_SHARED}/dags/" 2>/dev/null
        }
        restart_scheduler
        echo -e "${GREEN}? DAGs synced and scheduler restarted${NC}"
        ;;
    "plugins")
        echo -e "${YELLOW}?? Syncing plugins only...${NC}"
        rsync -av --delete --no-times --no-perms "${LOCAL_DEV}/plugins/" "${NFS_SHARED}/plugins/" 2>/dev/null || {
            rsync -av --no-times --no-perms "${LOCAL_DEV}/plugins/" "${NFS_SHARED}/plugins/" 2>/dev/null
        }
        restart_scheduler
        echo -e "${GREEN}? Plugins synced and scheduler restarted${NC}"
        ;;
    "config")
        echo -e "${YELLOW}??  Syncing configs only...${NC}"
        rsync -av --delete --no-times --no-perms "${LOCAL_DEV}/configs/" "${NFS_SHARED}/configs/" 2>/dev/null || {
            rsync -av --no-times --no-perms "${LOCAL_DEV}/configs/" "${NFS_SHARED}/configs/" 2>/dev/null
        }
        echo -e "${GREEN}? Configs synced${NC}"
        ;;
    "full")
        echo -e "${YELLOW}?? Full sync...${NC}"
        sync_directories
        sync_files
        fix_permissions
        restart_scheduler
        show_status
        ;;
    "watch")
        echo -e "${YELLOW}?? Watching for changes (Press Ctrl+C to stop)...${NC}"
        while true; do
            if [ "${LOCAL_DEV}/dags" -nt "${NFS_SHARED}/dags" ] || 
               [ "${LOCAL_DEV}/plugins" -nt "${NFS_SHARED}/plugins" ]; then
                echo -e "${BLUE}?? Changes detected, syncing...${NC}"
                sync_directories
                restart_scheduler
                echo -e "${GREEN}? Auto-sync completed at $(date)${NC}"
            fi
            sleep 10
        done
        ;;
    "status")
        show_status
        ;;
    "help"|"-h"|"--help")
        echo -e "${BLUE}Airflow Code Sync Script${NC}"
        echo ""
        echo -e "${YELLOW}Usage:${NC}"
        echo -e "  $0 [command]"
        echo ""
        echo -e "${YELLOW}Commands:${NC}"
        echo -e "  full       Full sync of all directories and files (default)"
        echo -e "  dags       Sync DAGs only and restart scheduler"
        echo -e "  plugins    Sync plugins only and restart scheduler"
        echo -e "  config     Sync configs only"
        echo -e "  watch      Watch for changes and auto-sync"
        echo -e "  status     Show sync status"
        echo -e "  help       Show this help message"
        echo ""
        echo -e "${YELLOW}Examples:${NC}"
        echo -e "  $0 dags           # Quick DAG sync"
        echo -e "  $0 full           # Complete sync"
        echo -e "  $0 watch          # Auto-sync mode"
        ;;
    *)
        echo -e "${RED}? Unknown command: $1${NC}"
        echo -e "Use '$0 help' for usage information"
        exit 1
        ;;
esac
