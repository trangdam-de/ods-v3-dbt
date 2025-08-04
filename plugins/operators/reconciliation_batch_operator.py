from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.sql_execute_query import CustomSQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
import logging
import psycopg
from datetime import timedelta
from helper1.context_utils import ensure_customer_batch_date 



def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

class ReconciliationBatchOperator(BaseOperator):
    @apply_defaults
    def __init__(self, 
                 filter_sql, 
                 filter_params=None, 
                 procedures=None, 
                 conn_id='staging_conn_id',
                 chunk_size=1000, # default chunk size for accntid_list
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filter_sql = filter_sql
        self.filter_params = filter_params or ()
        self.procedures = procedures or []
        self.conn_id = conn_id
        self.chunk_size = chunk_size

    def get_filter_params(self, context):
        if callable(self.filter_params):
            return self.filter_params(context)
        conf = context.get('dag_run').conf if context.get('dag_run') else None
        if conf and 'batch_date' in conf:
            batch_date = conf['batch_date']
        else:
            batch_date = context['data_interval_start'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y%m%d')
        return (batch_date,)

    def execute(self, context):
        logging.info("[ReconciliationBatchOperator] START batch reconciliation")
        conn_uri = BaseHook.get_connection(self.conn_id).get_uri()
        filter_params = self.get_filter_params(context)
        with psycopg.connect(conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(self.filter_sql, filter_params)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
        customers = [dict(zip(columns, row)) for row in rows]
        logging.info(f"[ReconciliationBatchOperator] Found {len(customers)} control periods")
        errors = []
        max_retry = 3
        accntid_list = [str(c['accntid']) for c in customers if c.get('accntid') is not None]
        accntid_filter_sql = self.filter_sql.strip()
        for proc in self.procedures:
            proc_name = proc['name']
            proc_sql = proc['sql']
            is_accntid_list = proc.get('is_accntid_list', False)
            chunk_size = proc.get('chunk_size', self.chunk_size)
            if is_accntid_list and customers and all('from_day' in c and 'to_day' in c for c in customers):
                from collections import defaultdict
                group_dict = defaultdict(list)
                for c in customers:
                    group_dict[(c['from_day'], c['to_day'])].append(c)
                group_list = list(group_dict.items())
                for group_idx, ((from_day, to_day), group_customers) in enumerate(group_list):
                    for idx, chunk in enumerate(chunk_list(group_customers, chunk_size)):
                        accntid_list = [str(c['accntid']) for c in chunk if c.get('accntid') is not None]
                        accntid_str = ','.join(accntid_list)
                        customer_chunk = dict(chunk[0]) if chunk else {}
                        customer_chunk['accntid'] = accntid_str
                        customer_chunk['accntid_list'] = accntid_list
                        customer_chunk['accntid_count'] = len(accntid_list)
                        customer_chunk['execution_date'] = filter_params[0]
                        customer_chunk['from_day'] = from_day
                        customer_chunk['to_day'] = to_day
                        customer_chunk['chunk_index'] = idx + 1
                        customer_chunk['chunk_total'] = (len(group_customers) + chunk_size - 1) // chunk_size
                        param_dict = proc['param_func'](customer_chunk)
                        self._execute_procedure_with_retry(proc_sql, param_dict, proc_name, customer_chunk, context, conn_uri, errors, max_retry)
            elif is_accntid_list:
                for idx, chunk in enumerate(chunk_list(customers, chunk_size)):
                    accntid_list = [str(c['accntid']) for c in chunk if c.get('accntid') is not None]
                    accntid_str = ','.join(accntid_list)
                    customer_chunk = dict(chunk[0]) if chunk else {}
                    customer_chunk['accntid'] = accntid_str
                    customer_chunk['accntid_list'] = accntid_list
                    customer_chunk['accntid_count'] = len(accntid_list)
                    customer_chunk['execution_date'] = filter_params[0]
                    customer_chunk['chunk_index'] = idx + 1
                    customer_chunk['chunk_total'] = (len(customers) + chunk_size - 1) // chunk_size
                    param_dict = proc['param_func'](customer_chunk)
                    self._execute_procedure_with_retry(proc_sql, param_dict, proc_name, customer_chunk, context, conn_uri, errors, max_retry)
            else:
                customer_no_accntid = {'execution_date': filter_params[0], 'accntid': None}
                param_dict = proc['param_func'](customer_no_accntid)
                customer_no_accntid['from_day'] = param_dict.get('fromdateid') or param_dict.get('fromdate') or param_dict.get('from_day')
                customer_no_accntid['to_day'] = param_dict.get('todateid') or param_dict.get('todate') or param_dict.get('to_day')
                self._execute_procedure_with_retry(proc_sql, param_dict, proc_name, customer_no_accntid, context, conn_uri, errors, max_retry)
        if errors:
            logging.error(f"Batch reconciliation: {len(errors)} errors. See ds.procedure_log for details. First 10 errors: {errors[:10]}")
        logging.info("[ReconciliationBatchOperator] END batch reconciliation")

    def _execute_procedure_with_retry(self, proc_sql, param_dict, proc_name, customer, context, conn_uri, errors, max_retry):
        from datetime import datetime
        start_time = datetime.now()
        retry_count = 0
        status = None
        error_message = None
        while retry_count < max_retry:
            try:
                with psycopg.connect(conn_uri) as conn:
                    with conn.cursor() as cur:
                        cur.execute(proc_sql, param_dict)
                        conn.commit()
                status = 'SUCCESS'
                break
            except Exception as e:
                retry_count += 1
                error_msg = f"[ERROR] customer_code={customer.get('customer_code')}, proc={proc_name}, error={str(e)}, retry={retry_count}/{max_retry}"
                logging.error(error_msg)
                error_message = str(e)
                if retry_count >= max_retry:
                    status = 'FAILED'
                    errors.append({'customer_code': customer.get('customer_code'), 'proc': proc_name, 'error': str(e)})
                else:
                    logging.info(f"Retrying customer_code={customer.get('customer_code')}, proc={proc_name}...")
        if status:
            self._log_procedure(status, proc_name, customer, context, start_time=start_time, error_message=error_message)

    def _log_procedure(self, status, proc_name, customer, context, start_time=None, error_message=None):
        conn_uri = BaseHook.get_connection(self.conn_id).get_uri()
        from_day = customer.get('from_day')
        to_day = customer.get('to_day')
        param_dict = None
        if not from_day or not to_day:
            for proc in self.procedures:
                if proc['name'] == proc_name:
                    param_dict = proc['param_func'](customer)
                    break
            if param_dict:
                if 'monthid' in param_dict:
                    from_day = to_day = str(param_dict['monthid'])
                else:
                    from_day = from_day or param_dict.get('fromdateid') or param_dict.get('fromdate') or param_dict.get('from_day')
                    to_day = to_day or param_dict.get('todateid') or param_dict.get('todate') or param_dict.get('to_day')
        if not from_day or not to_day:
            exec_date = context.get('execution_date')
            if exec_date:
                exec_str = exec_date.strftime('%Y%m%d')
                from_day = from_day or exec_str
                to_day = to_day or exec_str
        from datetime import datetime
        end_time = datetime.now()
        run_date = context['execution_date'] if hasattr(context['execution_date'], 'strftime') else None
        with psycopg.connect(conn_uri) as conn:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO ds.procedure_log 
                (dag_id, task_id, procedure_name, run_date, accnt_id, from_day, to_day, status, error_message, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql, (
                    context['dag'].dag_id,
                    context['task_instance'].task_id,
                    proc_name,
                    run_date,  # run_date là datetime, sẽ lưu cả ngày giờ
                    str(customer.get('accntid') or customer.get('accnt_id') or ''),
                    from_day,
                    to_day,
                    status,
                    error_message,
                    start_time,
                    end_time
                ))
                conn.commit()

