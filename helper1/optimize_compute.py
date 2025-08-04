import math
import multiprocessing as mp
import os
import psutil
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta 

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def get_optimal_settings(total_rows: int):
    """Calculate optimal chunk size and number of processes"""
    cpu_count = mp.cpu_count()
    available_memory = psutil.virtual_memory().available
    
    recommended_processes = max(1, int(cpu_count * 0.8))
    row_size_estimate = 1024  # bytes
    safe_memory = available_memory * 0.6
    max_chunk_size = int(safe_memory / (row_size_estimate * recommended_processes))
    optimal_chunk_size = min(50000, max_chunk_size)
    
    # Adjust chunk count to ensure even distribution
    total_chunks = math.ceil(total_rows / optimal_chunk_size)
    optimal_chunk_size = math.ceil(total_rows / total_chunks)
    
    return optimal_chunk_size, recommended_processes

def get_total_rows(engine,
                   schema_name: str,
                   table_name: str, 
                   cursor_field: str,
                   start_time: str,
                   end_time: str,
                   type: str):
    # engine = create_engine(f"mssql+pyodbc:///?odbc_connect={config.get_conn_str()}")
    # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    sql_query = f"""
                    SELECT count(1)
                    FROM "{schema_name}"."{table_name}"
                    """
    if type == 'oracle':
        codition = f"""
                WHERE {cursor_field} >= TO_DATE('{start_time}', 'YYYY-MM-DD HH24:MI:SS') 
                AND {cursor_field} < TO_DATE('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                """ if cursor_field else ""
        sql_query = sql_query + codition 
        result = engine.get_pandas_df(sql_query)
        return result.iloc[0, 0]
    elif type == 'mssql':
        codition = f"""
                WHERE {cursor_field} >= CONVERT(DATETIME, '{start_time}', 120) 
                AND {cursor_field} < CONVERT(DATETIME, '{end_time}', 120)
                """ if cursor_field else ""
        sql_query = sql_query + codition 
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            conn.close()
        return result.scalar()
    
def create_temp_dir():
    """Create temporary directory for chunk files"""
    temp_dir = f"temp_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

def branching_operator(key,
                       threshold,
                       source_connect,
                       schema_name,
                       table_name, 
                       cursor_field,
                       start_time,
                       end_time,
                       type):
    engine = MsSqlHook(mssql_conn_id=source_connect).get_sqlalchemy_connection()
    total_rows = get_total_rows(engine, 
                   schema_name, 
                   table_name, 
                   cursor_field,
                   start_time,
                   end_time,
                   type)
    print(f"total rows: {total_rows}")
    return f"one_to_one_tasks.{key}.source_to_staging_{key}"
    # return f"source_to_staging_parallel_{key}" if threshold < total_rows else f"source_to_staging_{key}"


