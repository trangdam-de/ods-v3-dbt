from airflow.plugins_manager import AirflowPlugin
from operators.khl_to_ods import KHLToStagingDailyOperator

class ODSPlugin(AirflowPlugin):
    name = "ods_plugin"
    operators = [KHLToStagingDailyOperator]