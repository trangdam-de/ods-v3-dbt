from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo
import psycopg

class TaskRunMetadata:
    def __init__(self, 
                 context,
                 table_name,
                 result_connection,
                 start_time: Optional[datetime] = None):
        
        self.task_id = context['ti'].task_id.split(".")[-1]
        self.dag_id = context['ti'].dag_id
        self.try_number = context['ti'].try_number
        self.start_time = start_time or datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        self.end_time: Optional[datetime] = None
        self.duration: Optional[float] = None
        self.result: Optional[Any] = None
        self.is_success: Optional[bool] = None

        self.table_name= table_name
        self.result_connection= result_connection
    
    def write_result(self, 
                     result: Any = None,
                     is_success: bool = None,
                     ) -> None:
        self.result = result
        self.is_success = is_success
        self.end_time = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        self.duration = (self.end_time - self.start_time).total_seconds()
        
        task_result = self.to_dict()
        columns = list(task_result.keys())
        values = list(task_result.values())
        
        conflict_columns = ['dag_id','task_id','execution_date']
        schema_name = "task_result"
        
        insert_query = f"""
        INSERT INTO {schema_name}.{self.table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        """
        """ON CONFLICT ({', '.join(conflict_columns)})
        DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])}
        """
        with psycopg.connect(self.result_connection) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, tuple(values))
            conn.commit()
            
        conn.close()
    
    def to_dict(self) -> dict:
        return {
            'task_id': self.task_id,
            'dag_id': self.dag_id,
            'run_day': self.start_time.date(),
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration,
            'result': self.result,
            'is_success': self.is_success,
            'try_number': self.try_number,
        }
        
