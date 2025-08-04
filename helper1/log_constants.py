# log_constants.py

class LogConstants:
    # Info-related log messages
    INFO_PRE_EXECUTE_START = "{task_id} - pre_execute() - START."
    INFO_PRE_EXECUTE_END = "{task_id} - pre_execute() - END."
    INFO_PRE_EXECUTE_DATAFRAME = "{task_id} - pre_execute() - DATAFRAME --> {dataframe}."
    INFO_EXECUTE_START = "{task_id} - execute() - START."
    INFO_EXECUTE_END = "{task_id} - execute() - END."
    INFO_POST_EXECUTE_START = "{task_id} - post_execute() - START."
    INFO_POST_EXECUTE_END = "{task_id} - post_execute() - END."
    INFO_PROCESS_CHUNK_MULTITHREAD_START = "{task_id} - process_chunks_multithreaded() - START."
    INFO_PROCESS_CHUNK_MULTITHREAD_END = "{task_id} - process_chunks_multithreaded() - END."
    INFO_PROCESS_CHUNK_START = "{task_id} - process_chunk() - START."
    INFO_PROCESS_CHUNK_END = "{task_id} - process_chunk() - END."
    INFO_LOAD_CHUNK_START = "{task_id} - load_chunks() - START."
    INFO_LOAD_CHUNK_END = "{task_id} - load_chunks() - END."

    # Schema validation
    VALIDATE_SCHEMA_START = "{task_id} - validate_schema() - START."
    VALIDATE_SCHEMA_END = "{task_id} - validate_schema() - END."
    VALIDATE_SCHEMA_ERROR = "{task_id} - validate_schema() - ERROR: {details}"

    # Data loading
    LOAD_DATA_START = "{task_id} - load_data() - START."
    LOAD_DATA_DATAFRAME = "{task_id} - load_data() - DATAFRAME --> {dataframe}."
    LOAD_DATA_END = "{task_id} - load_data() - END."
    LOAD_DATA_ERROR = "{task_id} - load_data() - ERROR: {details}."


