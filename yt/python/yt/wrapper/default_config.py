from __future__ import print_function

from . import common
from .config_remote_patch import RemotePatchableValueBase, RemotePatchableString, RemotePatchableBoolean, _validate_operation_link_pattern  # noqa
from .constants import DEFAULT_HOST_SUFFIX, SKYNET_MANAGER_URL, PICKLING_DL_ENABLE_AUTO_COLLECTION
from .mappings import VerifiedDict

import yt.logger as logger
import yt.yson as yson
import yt.json_wrapper as json
from yt.yson import YsonEntity, YsonMap

try:
    import yt.packages.six as six
except ImportError:
    import six

import os
import sys
from copy import deepcopy
from datetime import timedelta


# pydoc :: default_config :: begin

DEFAULT_WRITE_CHUNK_SIZE = 128 * common.MB


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
        #     Sleep for (1.0 + eps) * min(max_timeout, start_timeout * base ^ retry_attempt),
        #     where eps ~ U[0, decay_factor_bound]
        "policy": None,
        "constant_time": None,
        "exponential_policy": {
            "start_timeout": None,
            "base": None,
            "max_timeout": None,
            "decay_factor_bound": None
        },
    }

    config = VerifiedDict(template_dict=config_dict)
    config.update(**kwargs)
    return config


def retries_config(**kwargs):
    if "total_timeout" in kwargs and isinstance(kwargs["total_timeout"], timedelta):
        kwargs["total_timeout"] = kwargs["total_timeout"].total_seconds() * 1000.0

    config_dict = {
        "count": None,
        "enable": None,
        "backoff": retry_backoff_config(),
        "total_timeout": None,
        "additional_retriable_error_codes": []
    }

    config = VerifiedDict(template_dict=config_dict)
    config.update(**kwargs)
    return config


def get_dynamic_table_retries():
    return retries_config(enable=True, total_timeout=timedelta(minutes=10), backoff={
        "policy": "exponential",
        "exponential_policy": {
            "start_timeout": 5000,
            "base": 1.5,
            "max_timeout": 120000,
            "decay_factor_bound": 0.3,
        }
    })


default_config = {
    # "http" | "native" | "rpc" | None
    # If backend equals "http", then all requests will be done through http proxy and http_config will be used.
    # If backend equals "native", then all requests will be done through c++ bindings and driver_config will be used.
    # If backend equals "rpc", then all requests will be done through c++ bindings to rpc proxies,
    # driver_config will be used.
    # If backend equals None, then its value will be automatically detected.
    "backend": None,

    # Configuration of proxy connection.
    "proxy": {
        "url": None,

        # Aliases for proxy/url.
        # You can set aliases={"primary": "localhost":12345} and use cluster name "primary" as proxy url
        # in the same way as you can use "hahn" or "markov" as proxy url in production.
        # It is recommended to use only cluster name as alias for proxy url.
        "aliases": {},

        # use https for proxy.url (if no schema in proxy.url)
        "prefer_https": False,

        "ca_bundle_path": None,

        # Suffix appended to url if it is short.
        "default_suffix": DEFAULT_HOST_SUFFIX,

        # Use TVM-only API endpoints.
        "tvm_only": False,

        # Possible values are "gzip", "br" and "identity", by default we use "br" if
        # brotli is installed and "gzip" otherwise.
        "accept_encoding": None,
        "content_encoding": None,

        # Retries configuration for http requests.
        "retries": retries_config(enable=True, total_timeout=timedelta(minutes=2), backoff={
            "policy": "exponential",
            "exponential_policy": {
                "start_timeout": 2000,
                "base": 2,
                "max_timeout": 20000,
                "decay_factor_bound": 0.3
            }
        }),

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

        # String, requests.session.proxies. Use this proxies if and only if your have no direct access to yt balancer
        "http_proxy": None,
        "https_proxy": None,

        "force_ipv4": False,
        "force_ipv6": False,

        # Format of parameters serialization to HTTP header.
        # By default we use specified structured_data_format, otherwise we use YSON.
        "header_format": None,

        "force_tracing": False,

        # Enable using heavy proxies for heavy commands (write_*, read_*).
        # NB: this option can be overridden with settings from cluster
        "enable_proxy_discovery": RemotePatchableBoolean(True, "enable_proxy_discovery"),
        # Number of top unbanned proxies that would be used to choose random
        # proxy for heavy request.
        "number_of_top_proxies_for_random_choice": 5,
        # Part of url to get list of heavy proxies.
        "proxy_discovery_url": "hosts",
        # Timeout of proxy ban.
        "proxy_ban_timeout": 120 * 1000,

        # Link to operation in web interface.
        # NB: this option can be overridden with settings from cluster
        "operation_link_pattern": RemotePatchableString("{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details", "operation_link_template", _validate_operation_link_pattern),

        # Sometimes proxy can return incorrect or incomplete response.
        # This option enables checking response format for light requests.
        "check_response_format": True,

        # List of commands that use framing (cf. https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#framing).
        "commands_with_framing": [
            "read_table",
            "get_table_columnar_statistics",
            "get_job_input",
            "list_jobs",
        ],
    },

    # Parameters for dynamic table requests retries.
    "dynamic_table_retries": get_dynamic_table_retries(),

    # Maximum number of rows to sample without operation
    "max_row_count_for_local_sampling": 100,

    # Timeout for waiting for tablets to become ready.
    "tablets_ready_timeout": 60 * 1000,

    # Check interval for waiting for tablets to become ready.
    "tablets_check_interval": 0.1 * 1000,

    # This option enables logging on info level of all requests, indented for testing.
    "enable_request_logging": False,

    # This option allows to disable token.
    "enable_token": True,
    # This option enables checking that token is specified.
    "check_token": False,
    # This option allows to cache token value in client state.
    "cache_token": True,
    # If specified then token_path is ignored,
    # otherwise token extracted from file specified by token_path.
    "token": None,
    # $HOME/.yt/token by default
    "token_path": None,
    # This option enables receiving token automatically
    # using current session ssh secret.
    "allow_receive_token_by_current_ssh_session": True,
    # Tokens for receiving token by current ssh session.
    "oauth_client_id": "23b4f83306e3469abdee07054d307e7c",
    "oauth_client_secret": "87dcc81340254b12a4cecdfe34c6d387",
    # Set to yt.wrapper.tvm.ServiceTicketAuth(tvm_client).
    "tvm_auth": None,

    # Force using this version of api.
    "api_version": None,

    # Version of api for requests through http, None for use latest.
    # For native driver version "v3" by default.
    "default_api_version_for_http": "v4",
    "default_api_version_for_rpc": "v4",

    # Enables generating request id and passing it to native driver.
    "enable_passing_request_id_to_driver": False,

    # Username for native driver requests.
    "driver_user_name": None,

    # Driver configuration.
    "driver_config": None,

    # Logging configuration.
    "driver_logging_config": None,
    "enable_driver_logging_to_stderr": None,

    # Address resolver configuration.
    "driver_address_resolver_config": None,

    # YP service discovery configuration.
    "yp_service_discovery_config": None,

    # Path to driver config.
    # ATTENTION: It is compatible with native yt binary written in C++, it means
    # that config should be in YSON format and contain driver, logging and tracing configurations.
    # Do not use it for Yt client initialization, use driver_config instead.
    # Logging and tracing initialization would be executed only once for first initialization.
    "driver_config_path": None,

    # Path to file with additional configuration.
    "config_path": None,
    "config_format": "yson",

    # Path to document node on cluster with config patches. Some fields will be lazy changed with this one.
    "config_remote_patch_path": "//sys/client_config",
    # False means lazy config patching (at field access), True - patch at client start, None - do not patch at all
    "apply_remote_patch_at_start": False,

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
        # In this case artificial __init__.py is added when modules archive is created.
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
        # Enable wrapping of Python stdin and stdout streams to avoid their unintentional usage.
        "safe_stream_mode": True,
        # Protect stdout file descriptor from user writes. This differs from safe_stream_mode in
        # that this option can prevent C/C++ code from writing to stdout as well as Python code.
        # Available options:
        #  * redirect_to_stderr - everything written to stdout by user code will be redirected to stderr;
        #  * drop - user writes to stdout will be redirected to /dev/null;
        #  * close - stdout will be closed for user writes. Warning: may lead to errors that are hard to debug;
        #  * none - disable protection.
        "stdout_fd_protection": "redirect_to_stderr",
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
            "enable_auto_collection": PICKLING_DL_ENABLE_AUTO_COLLECTION,
            "library_filter": None
        },
        # Ignore client yt_yson_bindings if platform on the cluster differs from client platform.
        "ignore_yson_bindings_for_incompatible_platforms": True,
        # Enable using function name as operation title.
        "use_function_name_as_title": True,
        # Enable modules filtering if client and server OS/python versions are different.
        "enable_modules_compatibility_filter": False,
        # Compression level of archive with modules (from 1 to 9)
        "modules_archive_compression_level": 6,
        # Compression codec for archive with modules
        "modules_archive_compression_codec": "gzip",
        # Size of tar archives to split modules into
        "modules_chunk_size": 100 * common.MB,
        # Bypass artifacts cache for modules files.
        "modules_bypass_artifacts_cache": None,
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
    # This option could be enabled ONLY by explicit approval on yt-admin@ mail list.
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

    # When structured format is not specified we use YSON if bindings presented, otherwise we use JSON.
    # NB: JSON format loses typing for integer nodes.
    "structured_data_format": None,

    # This option forces using YSON as structured data format even if YSON bindings are missing. DEPRECATED.
    "force_using_yson_for_formatted_requests": False,

    # Attributes of automatically created tables.
    "create_table_attributes": None,

    # TODO(ignat): make sections about local temp and remote temp.
    # Remove temporary files after creation.
    "clear_local_temp_files": True,
    "local_temp_directory": None,

    # Path to remote directories for temporary files and tables.
    "remote_temp_files_directory": None,
    "remote_temp_tables_directory": "//tmp/yt_wrapper/table_storage",
    "remote_temp_tables_bucket_count": 200,

    # Expiration timeout for temporary objects (in milliseconds).
    "temp_expiration_timeout": 7 * 24 * 60 * 60 * 1000,

    "file_cache": {
        "replication_factor": 10,
    },
    "use_legacy_file_cache": None,

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
        "stderr_download_timeout": 60 * 1000,
        # Enables logging in text format operation failed error and job errors.
        "enable_logging_failed_operation": True,
        # Stderr encoding.
        "stderr_encoding": "utf-8",
        # Collect operation's stderr from all jobs (not just failed)
        "always_show_job_stderr": False,
    },

    "read_parallel": {
        # Number of threads for reading table.
        "max_thread_count": 10,
        # Approximate data size per one thread.
        "data_size_per_thread": 8 * 1024 * 1024,
        # Always run read parallel if it is possible.
        "enable": False
    },
    "write_parallel": {
        # Number of threads.
        "max_thread_count": None,  # automatically chosen
        # Always run parallel writing if it is possible.
        "enable": None,  # automatically chosen
        # An upper bound for the amount of memory that parallel write is allowed to use.
        # Note: if you specify both write_parallel/max_thread_count and write_retries/chunk_size,
        # this option is ignored.
        "memory_limit": 512 * common.MB,
        # This option allows to write table in unordered mode.
        "unordered": False,
        # The restriction on the number of chunks which will be passed to concatenate command.
        "concatenate_size": 100,
        # This option allows to save intermediate data in remote_temp_files_directory / remote_temp_tables_directory.
        "use_tmp_dir_for_intermediate_data": True
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
    # Default porto/docker layer to use in operations, coma separated string ("auto", "porto:auto", "docker:auto" has special meaning)
    "operation_base_layer": None,
    "base_layers_registry_path": "//images/base_layers",

    # Default value of table table writer configs.
    # It is passed to write_table and to job_io sections in operation specs.
    "table_writer": {
    },

    # Defaults that will be passed to all user job specs with the highest priority.
    "user_job_spec_defaults": {
    },

    # TODO(ignat): rename to attached_operation_mode = false
    # If detached False all operations run under special transaction. It causes operation abort if client died.
    "detached": True,

    # Prefix for all relative paths.
    #
    # If prefix is set to "//prefix/path" then request like
    #   yt.read_table("relative/path")
    # translates to
    #   yt.read_table("//prefix/path/relative/path")
    #
    # WARNING: select_rows command ignores this configuration parameter for tables inside query.
    # If there is such need a workaround can be used:
    #   yt.select_rows("... from [{}] ...".format(client.TablePath("relative/path/to/table")))
    "prefix": None,

    # Default timeout of transactions that started manually.
    "transaction_timeout": 30 * 1000,
    # How often wake up to determine whether transaction need to be pinged.
    "transaction_sleep_period": 100,

    # Deprecated!
    # Use signal (SIGUSR1) instead of KeyboardInterrupt in main thread if ping failed.
    # Signal is sent to main thread and YtTransactionPingError is raised inside
    # signal handler. The error is processed inside __exit__ block: it will be thrown
    # out to user, all transactions in nested context managers will be aborted.
    # Be careful! If Transaction is created not in main thread this will cause
    # error "ValueError: signal only works in main thread".
    "transaction_use_signal_if_ping_failed": None,

    # Action on failed ping. Valid values listed in yt.wrapper.transaction.PING_FAILED_MODES, default is "interrupt_main".
    "ping_failed_mode": None,

    # Function to call if ping_failed_mode is "call_function", no default (must be supplied by user).
    "ping_failed_function": None,

    # Default value of raw option in read, write, select, insert, lookup, delete.
    "default_value_of_raw_option": False,

    # Default value for enable_batch_mode option in search command.
    "enable_batch_mode_for_search": False,

    # Retries for read request. This type of retries parse data stream, if it is enabled, reading may be much slower.
    "read_retries": (retries_config(count=30, enable=True, total_timeout=None, backoff={
        "policy": "exponential",
        "exponential_policy": {
            "start_timeout": 2000,
            "base": 2,
            "max_timeout": 60000,
            "decay_factor_bound": 0.3
        }})
        .update_template_dict({
            "allow_multiple_ranges": True,
            "create_transaction_and_take_snapshot_lock": True,
            "change_proxy_period": None,
            "use_locked_node_id": True,
        })),

    # Retries for write commands. It split data stream into chunks and writes it separately under transactions.
    "write_retries": (retries_config(count=6, enable=True, total_timeout=None, backoff={
        "policy": "exponential",
        "exponential_policy": {
            "start_timeout": 30000,
            "base": 2,
            "max_timeout": 120000,
            "decay_factor_bound": 0.3
        }})
        .update_template_dict({
            "chunk_size": None,  # automatically chosen
            # Parent transaction wrapping whole write process.
            # If "transaction_id" is not specified it will be automatically created.
            "transaction_id": None,
            # Number of rows to build blobs that will be written to socket.
            "rows_chunk_size": 100,
        })),

    # Retries for start operation requests.
    # It may fail due to violation of cluster operation limit.
    "start_operation_retries": retries_config(enable=True, total_timeout=timedelta(hours=5), backoff={
        "policy": "exponential",
        "exponential_policy": {
            "start_timeout": 3000,
            "base": 2,
            "max_timeout": 60000,
            "decay_factor_bound": 0.3
        }}),
    "start_operation_request_timeout": 60000,

    # Retries for concatenate requests.
    "concatenate_retries": retries_config(enable=True, total_timeout=timedelta(minutes=10), backoff={
        "policy": "exponential",
        "exponential_policy": {
            "start_timeout": 20000,
            "base": 2,
            "max_timeout": 120000,
            "decay_factor_bound": 0.3,
        }
    }),

    # Parameters for get_operation request retries.
    "get_operation_retries": get_dynamic_table_retries(),

    # Special timeouts for commands retrieving information about operations,
    # i.e. 'list_operations', 'list_jobs', 'get_operation', 'get_job'.
    "operation_info_commands_timeout": 60000,

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
    "execute_batch_concurrency": 50,
    "batch_requests_retries": retries_config(enable=True, total_timeout=timedelta(minutes=10),
                                             backoff={"policy": "rounded_up_to_request_timeout"}),

    "skynet_manager_url": SKYNET_MANAGER_URL,

    "enable_logging_for_params_changes": False,

    "write_progress_bar": {
        "enable": None,
    },

    # NB: read_progress_bar actually shows not how much data is downloaded, but how much data
    # user code has read.
    # WARNING: progress-bar is destroyed when input stream is deallocated. If you keep holding
    # the reference, progress-bar will NOT be destroyed. This is why it is disabled by default.
    "read_progress_bar": {
        "enable": False,  # enabled in cli
    },

    "allow_fallback_to_native_driver": True,

    "started_by_command_length_limit": 4096,

    # Handle (some) type mismatch while reading, not initializing
    # i.e. do not throw exception while declare "weak" reading (f.e. read Optional[int] into int)
    # but throw excepion while reading bad data (if None really occurs)
    "runtime_type_validation": False,
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
    template_dict = deepcopy(default_config)
    template_dict["proxy"] = VerifiedDict(
        template_dict=template_dict["proxy"],
        keys_to_ignore=["aliases"],
        transform_func=transform_value)
    config = VerifiedDict(
        template_dict=template_dict,
        keys_to_ignore=["spec_defaults", "spec_overrides", "table_writer", "user_job_spec_defaults"],
        transform_func=transform_value)

    _update_from_env_vars(config, FORCED_SHORTCUTS)

    return config


FORCED_SHORTCUTS = {
    "BASE_LAYER" : "operation_base_layer",
}


SHORTCUTS = {
    "PROXY": "proxy/url",
    "PROXY_SUFFIX": "proxy/default_suffix",
    "PROXY_URL_ALIASING_CONFIG": "proxy/aliases",
    "TOKEN": "token",
    "TOKEN_PATH": "token_path",
    "USE_TOKEN": "enable_token",
    "CHECK_TOKEN": "check_token",
    "ACCEPT_ENCODING": "proxy/accept_encoding",
    "CONTENT_ENCODING": "proxy/content_encoding",
    "FORCE_IPV4": "proxy/force_ipv4",
    "FORCE_IPV6": "proxy/force_ipv6",

    "VERSION": "api_version",

    "DRIVER_CONFIG": "driver_config",
    "DRIVER_CONFIG_PATH": "driver_config_path",

    "USE_HOSTS": "proxy/enable_proxy_discovery",
    "HOSTS": "proxy/proxy_discovery_url",

    "ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE": "yamr_mode/always_set_executable_flag_on_files",
    "USE_MAPREDUCE_STYLE_DESTINATION_FDS": "yamr_mode/use_yamr_style_destination_fds",
    "TREAT_UNEXISTING_AS_EMPTY": "yamr_mode/treat_unexisting_as_empty",
    "DELETE_EMPTY_TABLES": "yamr_mode/delete_empty_tables",
    "USE_YAMR_SORT_REDUCE_COLUMNS": "yamr_mode/use_yamr_sort_reduce_columns",
    "REPLACE_TABLES_WHILE_COPY_OR_MOVE": "yamr_mode/replace_tables_on_copy_and_move",
    "CREATE_RECURSIVE": "yamr_mode/create_recursive",
    "THROW_ON_EMPTY_DST_LIST": "yamr_mode/throw_on_missing_destination",
    "RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED": "yamr_mode/run_map_reduce_if_source_is_not_sorted",
    "USE_NON_STRICT_UPPER_KEY": "yamr_mode/use_non_strict_upper_key",
    "CHECK_INPUT_FULLY_CONSUMED": "yamr_mode/check_input_fully_consumed",
    "FORCE_DROP_DST": "yamr_mode/abort_transactions_with_remove",
    "USE_YAMR_STYLE_PREFIX": "yamr_mode/use_yamr_style_prefix",

    "OPERATION_STATE_UPDATE_PERIOD": "operation_tracker/poll_period",
    "STDERR_LOGGING_LEVEL": "operation_tracker/stderr_logging_level",
    "IGNORE_STDERR_IF_DOWNLOAD_FAILED": "operation_tracker/ignore_stderr_if_download_failed",
    "KEYBOARD_ABORT": "operation_tracker/abort_on_sigint",

    "FILE_STORAGE": "remote_temp_files_directory",
    "LOCAL_TMP_DIR": "local_temp_directory",

    # Deprecated
    "TEMP_TABLES_STORAGE": "remote_temp_tables_directory",
    # Non-deprecated version of TEMP_TABLES_STORAGE
    "TEMP_DIR": "remote_temp_tables_directory",

    "PREFIX": "prefix",

    "POOL": "pool",
    "MEMORY_LIMIT": "memory_limit",
    "SPEC": "spec_defaults",
    "BASE_LAYER" : "operation_base_layer",
    "TABLE_WRITER": "table_writer",

    "RETRY_READ": "read_retries/enable",
    "USE_RETRIES_DURING_WRITE": "write_retries/enable",
    "USE_RETRIES_DURING_UPLOAD": "write_retries/enable",

    "CHUNK_SIZE": "write_retries/chunk_size",

    "DETACHED": "detached",

    "TABULAR_DATA_FORMAT": "tabular_data_format",

    "CONFIG_PATH": "config_path",
    "CONFIG_FORMAT": "config_format",

    "ARGCOMPLETE_VERBOSE": "argcomplete_verbose",

    "USE_YAMR_DEFAULTS": "yamr_mode/use_yamr_defaults",
    "IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST": "yamr_mode/ignore_empty_tables_in_mapreduce_list"
}


def update_config_from_env(config):
    # type: (yt.wrapper.mappings.VerifiedDict) -> yt.wrapper.mappings.VerifiedDict
    """Patch config from envs"""

    _update_from_env_patch(config)

    _update_from_file(config)

    _update_from_env_vars(config)

    return config


def _update_from_env_vars(config, shortcuts=None):
    # type: (yt.wrapper.mappings.VerifiedDict, dict | None) -> None

    def _get_var_type(value):
        var_type = type(value)
        # Using int we treat "0" as false, "1" as "true"
        if var_type == bool:
            try:
                value = int(value)
            except:  # noqa
                pass
        # None type is treated as str
        if isinstance(None, var_type):
            var_type = str
        elif var_type == dict or var_type == YsonMap:
            var_type = lambda obj: yson.json_to_yson(json.loads(obj)) if obj else {}  # noqa
        elif isinstance(value, RemotePatchableValueBase):
            var_type = type(value.value)

        return var_type

    def _apply_type(applied_type, key, value):
        try:
            return applied_type(value)
        except ValueError:
            raise common.YtError("Incorrect value of option '{0}': failed to apply type {1} to '{2}' of type {3}"
                                 .format(key, applied_type, value, type(value)))

    def _set(d, key, value):
        parts = key.split("/")
        for k in parts[:-1]:
            d = d[k]
        d[parts[-1]] = value

    def _get(d, key):
        parts = key.split("/")
        for k in parts:
            d = d.get(k)
        return d

    if not shortcuts:
        shortcuts = SHORTCUTS

    for key, value in six.iteritems(os.environ):
        prefix = "YT_"
        if not key.startswith(prefix):
            continue

        key = key[len(prefix):]
        if key in shortcuts:
            name = shortcuts[key]
            if name == "driver_config":
                var_type = yson.loads
            elif name == "proxy/aliases":
                def parse_proxy_aliases(value):
                    return yson.yson_to_json(yson.loads(value.encode()))
                var_type = parse_proxy_aliases
            else:
                var_type = _get_var_type(_get(config, name))
            # NB: it is necessary to set boolean variable as 0 or 1.
            if var_type is bool:
                value = int(value)
            _set(config, name, _apply_type(var_type, key, value))


def _update_from_env_patch(config):
    # type: (yt.wrapper.mappings.VerifiedDict) -> None

    if "YT_CONFIG_PATCHES" in os.environ:
        try:
            patches = yson._loads_from_native_str(os.environ["YT_CONFIG_PATCHES"],
                                                  yson_type="list_fragment",
                                                  always_create_attributes=False)
        except yson.YsonError:
            print("Failed to parse YT config patches from 'YT_CONFIG_PATCHES' environment variable", file=sys.stderr)
            raise

        try:
            for patch in reversed(list(patches)):
                common.update_inplace(config, patch)
        except:  # noqa
            print("Failed to apply config from 'YT_CONFIG_PATCHES' environment variable", file=sys.stderr)
            raise


def _update_from_file(config):
    # type: (yt.wrapper.mappings.VerifiedDict) -> None

    # These options should be processed before reading config file
    for opt_name in ["YT_CONFIG_PATH", "YT_CONFIG_FORMAT"]:
        if opt_name in os.environ:
            config[SHORTCUTS[opt_name[3:]]] = os.environ[opt_name]
    config_path = config["config_path"]

    if config_path is None:
        home = common.get_home_dir()

        config_path = "/etc/ytclient.conf"
        if home:
            home_config_path = os.path.join(os.path.expanduser("~"), ".yt/config")
            if os.path.isfile(home_config_path):
                config_path = home_config_path

        try:
            with open(config_path, "r"):
                pass
        except IOError:
            config_path = None

    if config_path and os.path.isfile(config_path):
        load_func = None
        format = config["config_format"]
        if format == "yson":
            load_func = yson.load
        elif format == "json":
            load_func = json.load
        else:
            raise common.YtError("Incorrect config_format '%s'" % format)
        try:
            with open(config_path, "rb") as f:
                common.update_inplace(config, load_func(f))
        except Exception:
            print("Failed to parse YT config from " + config_path, file=sys.stderr)
            raise


def get_config_from_env():
    # type: () -> yt.wrapper.mappings.VerifiedDict
    """Get default config with patches from envs"""
    return update_config_from_env(get_default_config())


def _get_settings_from_cluster_callback(config=None, client=None):
    import yt.wrapper as yt
    if config is None and client is None:
        # config=None, client=None
        client_or_module = yt
        config = client_or_module.config.config
    elif config is not None and client is None:
        # config=<VerifyedDict>, client=None
        client_or_module = yt.YtClient(config=config)
    elif client is not None:
        # config=None, client=<YtClient>
        client_or_module = client
        config = client.config
    RemotePatchableValueBase.patch_config_with_remote_data(client_or_module)
    RemotePatchableValueBase._materialize_all_leafs(config)
    logger.debug("Client config patched with data from cluster \"%s\": \"%s\"", config["config_remote_patch_path"], RemotePatchableValueBase._get_remote_cluster_data(client_or_module))
