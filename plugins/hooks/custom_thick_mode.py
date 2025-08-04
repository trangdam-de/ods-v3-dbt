from airflow.providers.oracle.hooks.oracle import OracleHook

class ThickModeOracleHook(OracleHook):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thick_mode = True  # Force thick mode