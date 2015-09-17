import common
from verified_dict import VerifiedDict

from copy import deepcopy

default_config = {
    # "http" | "native" | None
    # If backend equals "http", then all requests will be done through http proxy and http_config will be used.
    # If backend equals "native", then all requests will be done through c++ bindings and driver_config will be used.
    # If backend equals None, thenits value will be automatically detected.
    "backend": None,

    # Configuration of proxy connection.
    "proxy": {
        "url": None,
        # Suffix appended to url if it is short.
        "default_suffix": ".yt.yandex.net",

        "accept_encoding": "gzip, identity",
        # "gzip" | "identity"
        "content_encoding": "gzip",

        # Number of retries and timeout between retries.
        "request_retry_timeout": 20000,
        "request_retry_count": 6,

        # Heavy commands have increased timeout.
        "heavy_request_retry_timeout": 60000,

        # More retries in case of operation state discovery.
        "operation_state_discovery_retry_count": 100,

        # Forces backoff between consequent requests.
        # !!! It is not proxy specific !!!
        "request_backoff_time": None,

        "force_ipv4": False,
        "force_ipv6": False,

        # Format of header with yt parameters.
        # In new versions YT supports also "yson", that useful for passing unsinged int values.
        "header_format": "json",

        # Enable using heavy proxies for heavy commands (write_*, read_*).
        "enable_proxy_discovery": True,
        # Part of url to get list of heavy proxies.
        "proxy_discovery_url": "hosts",
        # Timeout of proxy ban.
        "proxy_ban_timeout": 120 * 1000,

        # Link to operation in web interface.
        "operation_link_pattern": "http://{proxy}/#page=operation&mode=detail&id={id}&tab=details",
    },

    # This option allows to disable token.
    "enable_token": True,
    # If token specified than token_path ignored,
    # otherwise token extracted from file specified by token_path.
    "token": None,
    # $HOME/.yt/token by default
    "token_path": None,

    # Version of api, None for use latest.
    "api_version": "v2",

    # Native driver config usually read from file.
    "driver_config_path": None,
    "driver_config": None,

    # Path to file with additional configuration.
    "config_path": None,
    "config_format": "yson",

    "pickling": {
        # Extensions to consider while looking files to archive.
        "search_extensions": None,
        # Function to filter modules.
        "module_filter": None,
        # Force using py-file even if pyc found.
        # It useful if local version of python differs from version installed on cluster.
        "force_using_py_instead_of_pyc": False,
        # Function to replace standard py_wrapper.create_modules_archive.
        "create_modules_archive_function": None,
        # Pickling framework used to save user modules.
        "framework": "dill",
        # Check that python version on local machine is the same as on cluster nodes.
        # Turn it off at your own risk.
        "check_python_version": False,
        # Path to python binary that would be used in jobs.
        "python_binary": "python"
    },

    "yamr_mode": {
        "always_set_executable_flag_on_files": False,
        "use_yamr_style_destination_fds": False,
        "treat_unexisting_as_empty": False,
        "delete_empty_tables": False,
        "use_yamr_sort_reduce_columns": False,
        "replace_tables_on_copy_and_move": False,
        "create_recursive": False,
        "throw_on_missing_destination": False,
        "run_map_reduce_if_source_is_not_sorted": False,
        "use_non_strict_upper_key": False,
        "check_input_fully_consumed": False,
        "abort_transactions_with_remove": False,
        "use_yamr_style_prefix": False
    },

    "tabular_data_format": None,

    # Remove temporary files after creation.
    "clear_local_temp_files": True,
    "local_temp_directory": "/tmp",

    # Path to remote directories for temporary files and tables.
    "remote_temp_files_directory": "//tmp/yt_wrapper/file_storage",
    "remote_temp_tables_directory": "//tmp/yt_wrapper/table_storage",

    "operation_tracker": {
        # Operation state check interval.
        "poll_period": 5000,
        # Log level used for print stderr messages.
        "stderr_logging_level": "INFO",
        # Ignore failures during stderr downloads.
        "ignore_stderr_if_download_failed": False,
        # Abort operation when SIGINT is received while waiting for the operation to finish.
        "abort_on_sigint": True,
        # Log job statistics on operation complete.
        "log_job_statistics": False
    },

    # Size of block to read from response stream.
    "read_buffer_size": 8 * 1024 * 1024,

    # Defaults that will be passed to all operation specs
    "spec_defaults": {
    },
    "memory_limit": None,

    # TODO(ignat): rename to attached_operaion_mode = false
    # If detached False all operations run under special transaction. It causes operation abort if client died.
    "detached": True,

    # Prefix for all relative paths.
    "prefix": "",

    # Default timeout of transactions that started manually.
    "transaction_timeout": 15 * 1000,
    # How often wake up to determine whether transaction need to be pinged.
    "transaction_sleep_period": 100,

    "write_file_as_one_chunk": True,

    # Default value of raw option in read, write, select, insert, lookup, delete.
    "default_value_of_raw_option": True,

    # Retries for read request. This type of retries parse data stream, if it is enabled, reading may be much slower.
    "read_retries": {
        "enable": False,
        "retry_count": 30,
        "create_transaction_and_take_snapshot_lock": True
    },

    # Retries for write commands. It split data stream into chunks and write it separately undef transactions.
    "write_retries": {
        "enable": True,
        # The size of data chunk that retried.
        "chunk_size": 512 * common.MB
    },

    "auto_merge_output": {
        # Action can be:
        # "none" - do nothing
        # "merge" - check output and merge chunks if necessary
        # "log" - check output and log result, do not merge
        "action": "log",
        "min_chunk_count": 1000,
        "max_chunk_size": 32 * common.MB
    }
}

def get_default_config():
    return VerifiedDict(["spec_defaults"], deepcopy(default_config))
