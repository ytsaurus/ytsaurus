from . import common
from .mappings import VerifiedDict

from yt.yson import YsonEntity

from copy import deepcopy
import tempfile

# pydoc :: default_config :: begin

def retry_backoff_config(**kwargs):
    config_dict = {
        # Backoff options for failed requests.
        # Supported policies:
        #   - rounded_up_to_request_timeout
        #     Sleep till request timeout (i.e. request timeout minus request duration).
        #     If request is heavy then sleep for heavy_request_timeout.
        #   - constant_time
        #     Sleep for duration specified in constant_time option.
        #   - exponential
        #     Sleep for min(max_timeout, start_timeout * base ^ retry_attempt).
        "policy": None,
        "constant_time": None,
        "exponential_policy": {
            "start_timeout": None,
            "base": None,
            "max_timeout": None,
        },
    }

    config = VerifiedDict(template_dict=config_dict)
    config.update(**kwargs)
    return config

def retries_config(**kwargs):
    config_dict = {
        "count": None,
        "enable": None,
        "backoff": retry_backoff_config(),
    }

    config = VerifiedDict(template_dict=config_dict)
    config.update(**kwargs)
    return config

default_config = {
    # "http" | "native" | None
    # If backend equals "http", then all requests will be done through http proxy and http_config will be used.
    # If backend equals "native", then all requests will be done through c++ bindings and driver_config will be used.
    # If backend equals None, thenits value will be automatically detected.
    "backend": None,

    # Option for backward compatibility
    "retry_backoff": retry_backoff_config(),

    # Configuration of proxy connection.
    "proxy": {
        "url": None,
        # Suffix appended to url if it is short.
        "default_suffix": ".yt.yandex.net",

        "accept_encoding": "gzip, identity",
        # "gzip" | "br" | "identity"
        "content_encoding": "gzip",

        # Options for backward compatibility
        "request_retry_count": None,
        "request_retry_enable": None,
        "request_retry_timeout": None,
        "heavy_request_retry_timeout": None,

        # Retries configuration for http requests.
        "retries": retries_config(count=6, enable=True, backoff={"policy": "rounded_up_to_request_timeout"}),

        # Timeout for connect.
        "connect_timeout": 5000,
        # Timeout for request.
        "request_timeout": 20000,
        # Heavy commands have increased timeout.
        "heavy_request_timeout": 60000,

        # Skip backoff in case of connect timeout error.
        "skip_backoff_if_connect_timed_out": True,

        # Increased retry count used for operation state discovery.
        "operation_state_discovery_retry_count": 100,

        # Forces backoff between consequent requests (for all requests, not just failed).
        # !!! It is not proxy specific !!!
        "request_backoff_time": None,

        "force_ipv4": False,
        "force_ipv6": False,

        # Format of header with yt parameters.
        # In new versions YT supports also "yson", that useful for passing unsinged int values.
        "header_format": None,

        # Enable using heavy proxies for heavy commands (write_*, read_*).
        "enable_proxy_discovery": True,
        # Number of top unbanned proxies that would be used to choose random
        # proxy for heavy request.
        "number_of_top_proxies_for_random_choice": 5,
        # Part of url to get list of heavy proxies.
        "proxy_discovery_url": "hosts",
        # Timeout of proxy ban.
        "proxy_ban_timeout": 120 * 1000,

        # Link to operation in web interface.
        "operation_link_pattern": "http://{proxy}/#page=operation&mode=detail&id={id}&tab=details",

        # Sometimes proxy can return incorrect or incomplete response. This option enables checking response format for light requests.
        "check_response_format": True,
    },

    # Parameters for dynamic table requests retries.
    "dynamic_table_retries": {
        "enable": True,
        "count": 6,
        "backoff": {
            "policy": "constant_time",
            "constant_time": 5 * 1000,
            "exponential_policy": {
                "start_timeout": None,
                "base": None,
                "max_timeout": None
            }
        }
    },

    # Timeout for waiting for tablets to become ready.
    "tablets_ready_timeout": 60 * 1000,

    # Check interval for waiting for tablets to become ready.
    "tablets_check_interval": 0.1 * 1000,

    # This option enables logging on info level of all requests.
    "enable_request_logging": False,

    # This option allows to disable token.
    "enable_token": True,
    # This option allows to cache token value in client state.
    "cache_token": True,
    # If token specified than token_path ignored,
    # otherwise token extracted from file specified by token_path.
    "token": None,
    # $HOME/.yt/token by default
    "token_path": None,
    # This option enables receiving token from oauth.yandex-team.ru
    # using currect session ssh secret.
    "allow_receive_token_by_current_ssh_session": False,
    # Tokens for receiving token by current ssh session.
    "oauth_client_id": "23b4f83306e3469abdee07054d307e7c",
    "oauth_client_secret": "87dcc81340254b12a4cecdfe34c6d387",

    # Force using this version of api.
    "api_version": None,

    # Version of api for requests through http, None for use latest.
    # For native driver version "v3" by default.
    "default_api_version_for_http": "v3",

    # If this option disabled we use JSON for formatted requests
    # if format is not specified and yson_bindings is not installed.
    # It allows to build python structures much faster than using YSON.
    # But in this case we loose type of integer nodes.
    "force_using_yson_for_formatted_requests": False,

    # Driver configuration.
    "driver_config": None,

    # Enables generating request id and passing it to native driver.
    "enable_passing_request_id_to_driver": False,

    # Username for native driver requests.
    "driver_user_name": None,

    # Path to driver config.
    # ATTENTION: It is comptatible with native yt binary written in C++, it means
    # that config should be in YSON format and contain driver, logging and tracing configurations.
    # Do not use it for Yt client initialization, use driver_config instead.
    # Logging and tracing initialization would be executed only once for first initialization.
    "driver_config_path": None,

    # Path to file with additional configuration.
    "config_path": None,
    "config_format": "yson",

    "pickling": {
        # Extensions to consider while looking files to archive.
        "search_extensions": None,
        # Function to filter modules for archive.
        "module_filter": None,
        # Force using py-file even if pyc found.
        # It useful if local version of python differs from version installed on cluster.
        "force_using_py_instead_of_pyc": False,
        # Some package modules are created in .pth files or manually in hooks during import.
        # For example, .pth file could be used to emulate namespace packages (see PEP-420).
        # Such packages can lack of __init__.py and sometimes can not be imported on nodes
        # (e.g. because .pth files can not be taken to nodes)
        # In this case artificial __init__.py is added when modules achive is created.
        "create_init_file_for_package_modules": True,
        # The list of files to add into archive. File should be specified as tuple that
        # consists of absolute file path and relative path in archive.
        "additional_files_to_archive": None,
        # Function to replace standard py_wrapper.create_modules_archive.
        # If this function specified all previous options does not applied.
        "create_modules_archive_function": None,
        # Logging level of module finding errors.
        "find_module_file_error_logging_level": "WARNING",
        # Pickling framework used to save user modules.
        "framework": "dill",
        # Forces dill to load additional types (e.g. numpy.ndarray) for better pickling
        # (has no effect if framework is not "dill")
        "load_additional_dill_types": False,
        # Check that python version on local machine is the same as on cluster nodes.
        # Turn it off at your own risk.
        "check_python_version": False,
        # Enables uploading local python to jobs and using it to run job.
        "use_local_python_in_jobs": None,
        # In local mode (if client and server are on the same node) this option enables job to
        # use local files without uploading them to cypress.
        # Possible values: False | True | None
        # If value is None then it will be auto-detected (basing on client and server fqdns)
        "enable_local_files_usage_in_job": None,
        # Command to run python in jobs, by default it is simple "python".
        "python_binary": None,
        # Enable wrapping of stdin and stdout streams to avoid their unintentional usage.
        "safe_stream_mode": True,
        # Age (in seconds) to distinguish currently modified modules and old modules.
        # These two types of modules would be uploaded separatly.
        # It is invented to descrease data uploaded to cluster
        # in case of consequence runs of the script with small modifications.
        "fresh_files_threshold": 3600,
        # Enables using tmpfs for modules archive.
        "enable_tmpfs_archive": True,
        # Add tmpfs archive size to memory limit.
        "add_tmpfs_archive_size_to_memory_limit": True,
        # Enable collecting different statistics of job.
        "enable_job_statistics": True,
        # Collect dependencies for shared libraries automatically. All dependencies listed by
        # ldd command (and not filtered by "library_filter") will be added to special dir in
        # job sandbox and LD_LIBRARY_PATH will be set accordingly.
        "dynamic_libraries": {
            "enable_auto_collection": False,
            "library_filter": None
        },
        # Ignore client yt_yson_bindings if platform on the cluster differs from client platform.
        "ignore_yson_bindings_for_incompatible_platforms": True,
        # Enable using function name as operation title.
        "use_function_name_as_title": True,
        # Enable modules filtering if client and server OS/python versions are different.
        "enable_modules_compatibility_filter": False
    },

    # Enables special behavior if client works with local mode cluster.
    # This behavior includes:
    #   - files are uploaded with replication factor equal to 1 by default
    #   - jobs use local files without uploading them to cluster.
    #     This is controlled by `pickling/enable_local_files_usage_in_job` option.
    #   - binary yson library is not required in job if format is YSON (python library will be allowed)
    # Possible values: False | True | None
    # If value is None client will use auto-detection.
    "is_local_mode": None,

    # By default HTTP requests to YT are forbidden inside jobs to avoid strange errors
    # and unnecessary cluster accesses.
    "allow_http_requests_to_yt_from_job": False,

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
        "use_yamr_style_prefix": False,
        "create_tables_outside_of_transaction": False,

        # Special option that enables 4Gb memory_limit, 4Gb data_size_per_job and default zlib_6 codec for
        # newly created tables. These defaults are similar to defaults on Yamr-clusters, but look inappropriate
        # for YT. Please do not use this option in new code. This option will be deleted after
        # migration to YT from Yamr. More discussion can found in YT-5220.
        "use_yamr_defaults": False,

        # Enables ignoring empty tables in mapreduce -list command.
        "ignore_empty_tables_in_mapreduce_list": False,

        # Enables setting schema on new tables.
        "create_schema_on_tables": False,

        # Run sorted merge instead of sort if input tables are sorted by sort_by prefix.
        "run_merge_instead_of_sort_if_input_tables_are_sorted": False,
    },

    "tabular_data_format": None,

    # Attributes of automatically created tables.
    "create_table_attributes": None,

    # TODO(ignat): make sections about local temp and remote temp.
    # Remove temporary files after creation.
    "clear_local_temp_files": True,
    "local_temp_directory": tempfile.gettempdir(),

    # Path to remote directories for temporary files and tables.
    "remote_temp_files_directory": "//tmp/yt_wrapper/file_storage",
    "remote_temp_tables_directory": "//tmp/yt_wrapper/table_storage",

    # Expiration timeout for temporary objects (in milliseconds).
    "temp_expiration_timeout": 7 * 24 * 60 * 60 * 1000,

    "file_cache": {
        "replication_factor": 10,
    },

    "operation_tracker": {
        # Operation state check interval.
        "poll_period": 5000,
        # Log level used for print stderr messages.
        "stderr_logging_level": "INFO",
        # Log level used for printing operation progress.
        "progress_logging_level": "INFO",
        # Ignore failures during stderr downloads.
        "ignore_stderr_if_download_failed": True,
        # Abort operation when SIGINT is received while waiting for the operation to finish.
        "abort_on_sigint": True,
        # Log job statistics on operation complete.
        "log_job_statistics": False,
        # Enable multithreading in stderr downloading.
        "stderr_download_threading_enable": True,
        # Number of threads for downloading jobs stderr messages.
        "stderr_download_thread_count": 10,
        # Timeout for downloading jobs stderr messages.
        # This parameter is only supported if stderr_download_threading_enable is True.
        "stderr_download_timeout": 60 * 1000
    },

    "read_parallel": {
        # Number of threads for reading table.
        "max_thread_count": 10,
        # Approximate data size per one thread.
        "data_size_per_thread": 8 * 1024 * 1024,
        # Always run read parallel if it is possible.
        "enable": False
    },

    # Size of block to read from response stream.
    "read_buffer_size": 8 * 1024 * 1024,

    # Defaults that will be passed to all operation specs with the least priority.
    "spec_defaults": {
    },
    # Defaults that will be passed to all operation specs with the highest priority.
    "spec_overrides": {
    },
    "memory_limit": None,
    "pool": None,

    # Default value of table table writer configs.
    # It is passed to write_table and to job_io sections in operation specs.
    "table_writer": {
    },

    # TODO(ignat): rename to attached_operaion_mode = false
    # If detached False all operations run under special transaction. It causes operation abort if client died.
    "detached": True,

    # Prefix for all relative paths.
    "prefix": None,

    # Default timeout of transactions that started manually.
    "transaction_timeout": 15 * 1000,
    # How often wake up to determine whether transaction need to be pinged.
    "transaction_sleep_period": 100,
    # Use signal (SIGUSR1) instead of KeyboardInterrupt in main thread if ping failed.
    # Signal is sent to main thread and YtTransactionPingError is raised inside
    # signal handler. The error is processed inside __exit__ block: it will be thrown
    # out to user, all transactions in nested context managers will be aborted.
    # Be careful! If Transaction is created not in main thread this will cause
    # error "ValueError: signal only works in main thread".
    "transaction_use_signal_if_ping_failed": False,

    # Default value of raw option in read, write, select, insert, lookup, delete.
    "default_value_of_raw_option": False,

    # Default value for enable_batch_mode option in search command.
    "enable_batch_mode_for_search": False,

    # Retries for read request. This type of retries parse data stream, if it is enabled, reading may be much slower.
    "read_retries": retries_config(count=30, enable=True, backoff={"policy": "rounded_up_to_request_timeout"})\
        .update_template_dict({
            "allow_multiple_ranges": False,
            "create_transaction_and_take_snapshot_lock": True,
            "retry_count": None,
            "change_proxy_period": None
        }),

    # Retries for write commands. It split data stream into chunks and writes it separately under transactions.
    "write_retries": retries_config(count=6, enable=True, backoff={"policy": "rounded_up_to_request_timeout"})\
        .update_template_dict({
            "chunk_size": 512 * common.MB,
            # Parent transaction wrapping whole write process.
            # If "transaction_id" is not specified it will be automatically created.
            "transaction_id": None,
        }),

    # Retries for start operation requests.
    # It may fail due to violation of cluster operation limit.
    "start_operation_retries": retries_config(count=30, enable=True, backoff={"policy": "rounded_up_to_request_timeout"})\
        .update_template_dict({
            # For backward compatibility.
            "retry_count": None,
            "retry_timeout": None
        }),
    "start_operation_request_timeout": 60000,

    "auto_merge_output": {
        # Action can be:
        # "none" - do nothing
        # "merge" - check output and merge chunks if necessary
        # "log" - check output and log result, do not merge
        "action": "log",
        "min_chunk_count": 1000,
        "max_chunk_size": 32 * common.MB
    },

    # Enable printing argcomplete errors.
    "argcomplete_verbose": False,

    # Do not fail with resolve error (just print warning) in yt.search(root, ...)
    # if root path does not exist.
    "ignore_root_path_resolve_error_in_search": False,

    # Options for transform function.
    "transform_options": {
        "chunk_count_to_compute_compression_ratio": 1,
        "desired_chunk_size": 2 * 1024 ** 3,
        "max_data_size_per_job": 16 * 1024 ** 3,
    },

    # Enables mounting sandbox in tmpfs. Automatically calculates file sizes and adds them to memory limit.
    "mount_sandbox_in_tmpfs": {
        "enable": False,
        # Additional tmpfs size (in bytes) to reserve for user data.
        "additional_tmpfs_size": 0
    },
    "max_batch_size": 100,
    "batch_requests_retries": retries_config(count=6, enable=True, backoff={"policy": "rounded_up_to_request_timeout"})
}

# pydoc :: default_config :: end

def transform_value(value, original_value):
    if original_value is False or original_value is True:
        if isinstance(value, str):
            raise TypeError("Value must be boolean instead of string")
    if isinstance(value, YsonEntity):
        return None
    return value

def get_default_config():
    """Returns default configuration of python API."""
    return VerifiedDict(
        template_dict=deepcopy(default_config),
        keys_to_ignore=["spec_defaults", "spec_overrides", "table_writer"],
        transform_func=transform_value)
