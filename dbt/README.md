# DBT Integration for ODS ETL System

## 🎯 Overview
This setup integrates DBT (Data Build Tool) into the existing ODS ETL pipeline to standardize transformations, improve data quality testing, and enhance maintainability.

## 📁 Directory Structure
```
dbt/
├── dbt_project.yml          # Main DBT project configuration
├── profiles/
│   └── profiles.yml          # Database connection profiles
├── models/
│   ├── sources.yml           # Source table definitions
│   ├── staging/
│   │   └── casreport/        # Staging models for CASREPORT
│   └── marts/
│       └── casreport/        # Final business models
├── macros/
│   └── transformations.sql  # Custom transformation macros
├── tests/                    # Custom data quality tests
└── packages.yml             # DBT package dependencies
```

## 🚀 Quick Start

### 1. Deploy the Stack
```bash
# Deploy updated docker-compose with DBT container
docker stack deploy -c docker-compose-swarm.yml airflow
```

### 2. Setup DBT
```bash
# Run the setup script
./setup-dbt.sh
```

### 3. Verify Installation
```bash
# Check DBT container
docker ps --filter label=com.docker.swarm.service.name=airflow_dbt

# Test DBT connection
docker exec -it $(docker ps -q --filter label=com.docker.swarm.service.name=airflow_dbt) \
    bash -c "cd /usr/app/dbt && dbt debug"
```

## 🔧 DBT Commands

### Basic Commands
```bash
# Enter DBT container
docker exec -it $(docker ps -q --filter label=com.docker.swarm.service.name=airflow_dbt) bash

# Install dependencies
dbt deps

# Test database connection
dbt debug

# Compile models
dbt compile

# Run specific models
dbt run --select staging.casreport
dbt run --select marts.casreport

# Run tests
dbt test --select staging.casreport

# Generate documentation
dbt docs generate
dbt docs serve --port 8081
```

### Advanced Commands
```bash
# Run with full refresh
dbt run --full-refresh --select dim_casreport_services

# Run specific model and downstream
dbt run --select stg_casreport__v_prd_srv+

# Run models with specific tags
dbt run --select tag:casreport

# Test specific columns
dbt test --select dim_casreport_services
```

## 🔄 Migration from Current System

### Current SQL Template Migration
Replace this pattern in DAGs:
```python
# OLD: SQLExecuteQueryOperator
update_des_table = SQLExecuteQueryOperator(
    task_id=f"update_des_table_{key}",
    conn_id=value.get('staging_conn_id'),
    sql=f"/sql/casreport/update_des_table/{key}.sql",
    params={"des_schema": ..., "des_table": ...}
)
```

With this pattern:
```python
# NEW: DbtRunOperator
dbt_transform = DbtRunOperator(
    task_id=f"dbt_transform_{key}",
    dir="/usr/app/dbt",
    profiles_dir="/root/.dbt",
    models=f"casreport.{key}",
    target="production"
)
```

### File Mapping
| Current SQL File | DBT Model | Description |
|------------------|-----------|-------------|
| `casreport_v_prd_srv.sql` | `dim_casreport_services.sql` | Service dimension |
| `casreport_d_row_item.sql` | `stg_casreport__d_row_item.sql` | Row items staging |
| `casreport_settlements.sql` | `fact_casreport_settlements.sql` | Settlement facts |

## 🧪 Testing Framework

### Basic Tests (Automated)
- `not_null`: Ensures no null values
- `unique`: Ensures uniqueness
- `relationships`: Referential integrity

### Custom Tests
```sql
-- Example: Custom business rule test
SELECT * FROM {{ ref('dim_casreport_services') }}
WHERE prd_srv_code NOT LIKE 'SRV%'
```

### Running Tests
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select dim_casreport_services

# Run tests with specific severity
dbt test --severity warn
```

## 📊 Configuration

### Environments
- **Development**: `dbt_dev` schema for testing
- **Staging**: `dbt_staging` schema for validation
- **Production**: Default schema for live data

### Variables
Configure in `dbt_project.yml`:
```yaml
vars:
  casreport:
    source_schema: "staging"
    target_schema: "casreport"
    source_prefix: "casreport_"
```

## 🔍 Monitoring and Logging

### DBT Logs
- Location: `/usr/app/logs/dbt.log`
- Airflow logs: `/opt/airflow/logs/`

### Performance Monitoring
```sql
-- Query to check model run times
SELECT 
    model_name,
    execution_time,
    rows_affected,
    run_started_at
FROM dbt_run_results
ORDER BY execution_time DESC;
```

## 🚨 Troubleshooting

### Common Issues

1. **Connection Errors**
   ```bash
   # Check database connectivity
   dbt debug
   
   # Verify profiles.yml
   cat /root/.dbt/profiles.yml
   ```

2. **Model Compilation Errors**
   ```bash
   # Check syntax
   dbt parse
   
   # Compile specific model
   dbt compile --select model_name
   ```

3. **Test Failures**
   ```bash
   # Run specific test with details
   dbt test --select test_name --store-failures
   
   # Check failed test results
   SELECT * FROM dbt_test_failures;
   ```

### Performance Optimization
```bash
# Use multiple threads
dbt run --threads 4

# Run incremental models only
dbt run --select config.materialized:incremental

# Skip tests for faster runs
dbt run --exclude test_type:data
```

## 📈 Next Steps

1. **Phase 1** (Week 1-2): Complete CASREPORT migration
2. **Phase 2** (Week 3-4): Add BCCP and CMS models  
3. **Phase 3** (Week 5-6): Advanced analytics and cross-source models
4. **Phase 4** (Week 7-8): Full Airflow DAG integration

## 🛠️ Development Workflow

1. **Model Development**
   ```bash
   # Create new model
   vim models/staging/casreport/stg_casreport__new_table.sql
   
   # Test locally
   dbt run --select stg_casreport__new_table
   dbt test --select stg_casreport__new_table
   ```

2. **Version Control**
   ```bash
   # Commit changes
   git add dbt/
   git commit -m "Add new CASREPORT model"
   git push origin main
   ```

3. **Deploy to Production**
   ```bash
   # Sync code (using existing sync script)
   ./sync-code.sh dbt
   
   # Deploy via Airflow DAG
   # Trigger casreport_dbt_dag
   ```

## 📚 Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [DBT Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Airflow-DBT Integration](https://github.com/gocardless/airflow-dbt)

## 💡 Tips

- Use `dbt --help` for command options
- Leverage DBT macros for reusable transformations  
- Set up DBT Cloud or external docs hosting for team collaboration
- Monitor query performance and optimize incremental models
- Use DBT packages for advanced functionality
