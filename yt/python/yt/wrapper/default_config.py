from __future__ import print_function

from typing import Optional, Tuple, TypedDict, Union, Literal, Dict, List, Any

from . import common
from .config_remote_patch import (RemotePatchableValueBase, RemotePatchableString, RemotePatchableBoolean,
                                  RemotePatchableInteger, _validate_operation_link_pattern,
                                  _validate_query_link_pattern)
from .constants import DEFAULT_HOST_SUFFIX, SKYNET_MANAGER_URL, PICKLING_DL_ENABLE_AUTO_COLLECTION, ENCRYPT_PICKLE_FILES, STARTED_BY_COMMAND_LENGTH_LIMIT
from .errors import YtConfigError
from .mappings import VerifiedDict

import yt.logger as logger
import yt.yson as yson
import yt.json_wrapper as json
from yt.yson import YsonEntity, YsonMap

import os
from copy import deepcopy
from datetime import timedelta


# pydoc :: default_config :: begin

DEFAULT_WRITE_CHUNK_SIZE = 128 * common.MB
DEFAULT_GLOBAL_CONFIG_PATH = "/etc/ytclient.conf"
DEFAULT_USER_CONFIG_REL_PATH = ".yt/config"


class DefaultConfigRetriesBackoffType(TypedDict, total=False):
    policy: Optional[Union[Literal["rounded_up_to_request_timeout"], Literal["constant_time"], Literal["exponential"]]]
    constant_time: Optional[int]

    class DefaultConfigRetriesBackoffExponentialPolicyType(TypedDict, total=False):
        start_timeout: int
        base: int
        max_timeout: int
        decay_factor_bound: float

    exponential_policy: Optional[DefaultConfigRetriesBackoffExponentialPolicyType]


class DefaultConfigRetriesType(TypedDict, total=False):
    count: Optional[int]
    enable: bool
    backoff: Optional[DefaultConfigRetriesBackoffType]
    total_timeout: Union[None, timedelta, int, float]
    additional_retriable_error_codes: Optional[List[Any]]


class DefaultConfigType(TypedDict, total=False):
    backend: Union[Literal["http"], Literal["rpc"], Literal["native"]]

    class DefaultConfigProxyType(TypedDict, total=False):
        url: str
        aliases: Dict[str, str]
        http_proxy_role: Optional[str]
        rpc_proxy_role: Optional[str]
        allow_light_proxy_for_heavy_requests: bool
        network_name: Optional[str]
        prefer_https: bool
        ca_bundle_path: Optional[str]
        default_suffix: Optional[str]
        tvm_only: bool
        accept_encoding: Optional[Union[Literal["gzip"], Literal["br"], Literal["identity"]]]
        content_encoding: Optional[str]
        retries: DefaultConfigRetriesType
        connect_timeout: int
        request_timeout: int
        heavy_request_timeout: int
        skip_backoff_if_connect_timed_out: bool
        operation_state_discovery_retry_count: int
        request_backoff_time: Optional[Any]
        http_proxy: Optional[str]
        https_proxy: Optional[str]
        force_ipv4: bool
        force_ipv6: bool
        header_format: Optional[str]
        force_tracing: bool
        enable_proxy_discovery: bool
        number_of_top_proxies_for_random_choice: int
        proxy_discovery_url: str
        proxy_ban_timeout: int
        operation_link_pattern: str
        query_link_pattern: str
        check_response_format: bool
        commands_with_framing: List[str]

    proxy: DefaultConfigProxyType
    dynamic_table_retries: DefaultConfigRetriesType
    max_row_count_for_local_sampling: int
    tablets_ready_timeout: int
    tablets_check_interval: float
    enable_request_logging: bool
    enable_token: bool
    check_token: bool
    cache_token: bool
    token: Optional[str]
    token_path: Optional[str]
    allow_receive_token_by_current_ssh_session: bool
    oauth_client_id: str
    oauth_client_secret: str
    tvm_auth: Optional[Any]

    class DefaultConfigAuthClassType(TypedDict, total=False):
        module_name: Optional[str]
        class_name: Optional[str]
        config: Optional[Any]

    auth_class: DefaultConfigAuthClassType
    auth: Optional[Any]
    impersonation_user: Optional[str]
    api_version: Optional[str]
    default_api_version_for_http: str
    default_api_version_for_rpc: str
    enable_passing_request_id_to_driver: bool
    driver_user_name: Optional[str]
    driver_config: Optional[Dict[str, Any]]
    driver_logging_config: Optional[Any]
    enable_driver_logging_to_stderr: Optional[Any]
    driver_address_resolver_config: Optional[Any]
    yp_service_discovery_config: Optional[Any]
    driver_config_path: Optional[str]
    config_path: Optional[str]
    config_format: str
    config_profile: Optional[str]
    config_remote_patch_path: str
    apply_remote_patch_at_start: bool

    class DefaultConfigPicklingType(TypedDict, total=False):
        search_extensions: Optional[Any]
        module_filter: Optional[Any]
        force_using_py_instead_of_pyc: bool
        create_init_file_for_package_modules: bool
        additional_files_to_archive: Optional[Any]
        create_modules_archive_function: Optional[Any]
        find_module_file_error_logging_level: str
        framework: str
        load_additional_dill_types: bool
        pickler_kwargs: List[Any]
        check_python_version: bool
        use_local_python_in_jobs: Optional[Any]
        enable_local_files_usage_in_job: Optional[Any]
        python_binary: Optional[Any]
        safe_stream_mode: bool
        stdout_fd_protection: str
        redirect_stdout_to_stderr: bool
        enable_tmpfs_archive: bool
        add_tmpfs_archive_size_to_memory_limit: bool
        enable_job_statistics: bool

        class DefaultConfigPicklingDynamicLibrariesType(TypedDict, total=False):
            enable_auto_collection: bool
            library_filter: Optional[Any]

        dynamic_libraries: DefaultConfigPicklingDynamicLibrariesType
        ignore_yson_bindings_for_incompatible_platforms: bool
        use_function_name_as_title: bool
        enable_modules_compatibility_filter: bool
        modules_archive_compression_level: int
        modules_archive_compression_codec: str
        modules_chunk_size: int
        modules_bypass_artifacts_cache: Optional[Any]
        ignore_system_modules: bool
        system_module_patterns: List[Any]
        encrypt_pickle_files: int

    pickling: DefaultConfigPicklingType
    is_local_mode: Optional[Any]
    allow_http_requests_to_yt_from_job: bool

    class DefaultConfigYamrModeType(TypedDict, total=False):
        always_set_executable_flag_on_files: bool
        use_yamr_style_destination_fds: bool
        treat_unexisting_as_empty: bool
        delete_empty_tables: bool
        use_yamr_sort_reduce_columns: bool
        replace_tables_on_copy_and_move: bool
        create_recursive: bool
        throw_on_missing_destination: bool
        run_map_reduce_if_source_is_not_sorted: bool
        use_non_strict_upper_key: bool
        check_input_fully_consumed: bool
        abort_transactions_with_remove: bool
        use_yamr_style_prefix: bool
        create_tables_outside_of_transaction: bool
        use_yamr_defaults: bool
        ignore_empty_tables_in_mapreduce_list: bool
        create_schema_on_tables: bool
        run_merge_instead_of_sort_if_input_tables_are_sorted: bool

    yamr_mode: DefaultConfigYamrModeType
    tabular_data_format: Optional[Any]
    structured_data_format: Optional[Any]
    force_using_yson_for_formatted_requests: bool
    create_table_attributes: Optional[Any]
    clear_local_temp_files: bool
    local_temp_directory: Optional[Any]
    remote_temp_files_directory: Optional[Any]
    remote_temp_tables_directory: str
    remote_temp_tables_bucket_count: int
    temp_expiration_timeout: int
    max_replication_factor: int

    class DefaultConfigFileCacheType(TypedDict, total=False):
        replication_factor: int

    file_cache: DefaultConfigFileCacheType
    use_legacy_file_cache: Optional[Any]

    class DefaultConfigOperationTrackerType(TypedDict, total=False):
        poll_period: int
        stderr_logging_level: str
        progress_logging_level: str
        ignore_stderr_if_download_failed: bool
        abort_on_sigint: bool
        log_job_statistics: bool
        stderr_download_threading_enable: bool
        stderr_download_thread_count: int
        stderr_download_timeout: int
        enable_logging_failed_operation: bool
        stderr_encoding: str
        always_show_job_stderr: bool

    operation_tracker: DefaultConfigOperationTrackerType

    class DefaultConfigQueryTrackerType(TypedDict, total=False):
        poll_period: int
        stderr_logging_level: str
        progress_logging_level: str
        abort_on_sigint: bool

    query_tracker: DefaultConfigQueryTrackerType

    class DefaultConfigReadParallelType(TypedDict, total=False):
        max_thread_count: int
        data_size_per_thread: int
        enable: bool

    read_parallel: DefaultConfigReadParallelType

    class DefaultConfigWriteParallelType(TypedDict, total=False):
        max_thread_count: Optional[Any]
        enable: Optional[Any]
        memory_limit: int
        unordered: bool
        concatenate_size: int
        use_tmp_dir_for_intermediate_data: bool

    write_parallel: DefaultConfigWriteParallelType
    read_buffer_size: int
    read_omit_inaccessible_columns: Optional[Any]
    spec_defaults: Dict[str, Any]
    spec_overrides: Dict[str, Any]
    memory_limit: Optional[Any]
    pool: Optional[Any]
    operation_base_layer: Optional[Any]
    base_layers_registry_path: str
    table_writer: Dict[str, Any]
    user_job_spec_defaults: Dict[str, Any]
    detached: bool
    prefix: Optional[Any]
    transaction_timeout: int
    transaction_sleep_period: int
    transaction_use_signal_if_ping_failed: Optional[Any]
    ping_failed_mode: Optional[Any]
    ping_failed_function: Optional[Any]
    default_value_of_raw_option: bool
    enable_batch_mode_for_search: bool

    class DefaultConfigReadRetriesType(DefaultConfigRetriesType, total=False):
        allow_multiple_ranges: bool
        create_transaction_and_take_snapshot_lock: bool
        change_proxy_period: Optional[Any]
        use_locked_node_id: bool

    read_retries: DefaultConfigReadRetriesType

    class DefaultConfigWriteRetriesType(DefaultConfigRetriesType, total=False):
        chunk_size: Optional[Any]
        transaction_id: Optional[Any]
        rows_chunk_size: int

    write_retries: DefaultConfigWriteRetriesType
    start_operation_retries: DefaultConfigRetriesType
    start_operation_request_timeout: int
    concatenate_retries: DefaultConfigRetriesType
    get_operation_retries: DefaultConfigRetriesType
    operation_info_commands_timeout: int

    class DefaultConfigAutoMergeOutputType(TypedDict, total=False):
        action: str
        min_chunk_count: int
        max_chunk_size: int

    auto_merge_output: DefaultConfigAutoMergeOutputType
    argcomplete_verbose: bool
    ignore_root_path_resolve_error_in_search: bool

    class DefaultConfigTransformOptionsType(TypedDict, total=False):
        chunk_count_to_compute_compression_ratio: int
        desired_chunk_size: int
        max_data_size_per_job: int

    transform_options: DefaultConfigTransformOptionsType

    class DefaultConfigMountSandboxInTmpfsType(TypedDict, total=False):
        enable: bool
        additional_tmpfs_size: int

    mount_sandbox_in_tmpfs: DefaultConfigMountSandboxInTmpfsType
    max_batch_size: int
    execute_batch_concurrency: int
    batch_requests_retries: DefaultConfigRetriesType
    skynet_manager_url: str
    enable_logging_for_params_changes: bool

    class DefaultConfigWriteProgressBarType(TypedDict, total=False):
        enable: Optional[Any]

    write_progress_bar: DefaultConfigWriteProgressBarType

    class DefaultConfigReadProgressBarType(TypedDict, total=False):
        enable: bool

    read_progress_bar: DefaultConfigReadProgressBarType
    allow_fallback_to_native_driver: bool
    started_by_command_length_limit: int
    runtime_type_validation: bool
    strawberry_ctl_address: str
    strawberry_cluster_name: Optional[str]

    class DefaultConfigUploadTableOptionsType(TypedDict, total=False):
        write_arrow_batch_size: int

    upload_table_options: DefaultConfigUploadTableOptionsType

    class DefaultConfigDumpTableOptionsType(TypedDict, total=False):
        min_batch_row_count: int

    dump_table_options: DefaultConfigDumpTableOptionsType
    enable_password_strength_validation: bool


def retry_backoff_config(**kwargs) -> DefaultConfigRetriesBackoffType:
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


def retries_config(**kwargs) -> DefaultConfigRetriesType:
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


def get_dynamic_table_retries() -> DefaultConfigRetriesBackoffType:
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

        # Proxy role to use in heavy proxies discovering based on the backend type.
        # If it is set to None, the default will be used.
        # NB: If the "proxy_discovery_url" config option is set, it is assumed to take priority over "http_proxy_role" option.
        "http_proxy_role": None,
        "rpc_proxy_role": None,

        # For testing purposes only.
        # If there are no heavy proxies, we can fallback to the light proxy.
        "allow_light_proxy_for_heavy_requests": True,

        # The name of the network from which the address will be returned during proxy discovery.
        "network_name": None,

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
        # Deprecated! It's recommended to use the 'http_proxy_role' option instead for configuring proxies.
        "proxy_discovery_url": RemotePatchableString("hosts", "http_proxy_discovery_url"),
        # Timeout of proxy ban.
        "proxy_ban_timeout": 120 * 1000,

        # Link to operation in web interface.
        # NB: this option can be overridden with settings from cluster
        # TODO(max42): this default is prehistorically obsolete and can be changed to something actual.
        "operation_link_pattern": RemotePatchableString("{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details", "operation_link_template", _validate_operation_link_pattern),

        # Link to query in web interface.
        # NB: this option can be overridden with settings from cluster
        "query_link_pattern": RemotePatchableString("{proxy}/queries/{id}", "query_link_template", _validate_query_link_pattern),

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
    # DEPRECATED: use `auth` instead of it.
    "tvm_auth": None,
    # Set to yt.wrapper.tvm.ServiceTicketAuth(tvm_client), yt.wrapper.tvm.UserTicketFixedAuth() or other implementation of yt.packages.requests.auth.AuthBase.
    "auth": None,
    # Description of auth class derived from AuthBase.
    "auth_class": {
        "module_name": None,
        "class_name": None,
        # If specified, it will be passed in the `class_name` constructor.
        "config": None,
    },
    # This option allows the client to impersonate another user.
    # Setting this option is only allowed for superusers that are not banned,
    # all other attempts at impersonation will result in an authorization error.
    # For now only the HTTP driver supports option, in the RPC driver it is ignored.
    # If you are using the native driver, use the `driver_user_name` option below instead.
    "impersonation_user": None,

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
    # The profile's name in the config.
    # https://github.com/ytsaurus/ytsaurus/issues/90
    "config_profile": None,

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
        # Additional arguments that will be passed to pickler.
        # Expected list of dicts `{"key": <param_name>, "value": <param_value>}`
        "pickler_kwargs": [],
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
        # Any stdout output will be redirected to stderr (by cluster, not client). "use_yamr_descriptors" has priority over "redirect_stdout_to_stderr"
        "redirect_stdout_to_stderr": True,
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
            "enable_auto_collection": RemotePatchableBoolean(PICKLING_DL_ENABLE_AUTO_COLLECTION, "python_pickling_dynamic_libraries_enable_auto_collection"),
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
        # Ignore "system" python modules (installed on client's host and presented in YT runtime).
        "ignore_system_modules": RemotePatchableBoolean(False, "python_pickling_ignore_system_modules"),
        "system_module_patterns": [
            r"/lib/python[\d\.]+/(site|dist)-packages/",
            r"/lib/python[\d\.]+/.+\.(py|pyc|so)$",
        ],
        # Encrypt files with pickle data (None - disabled, 1 - enabled, 2 - enabled with key in "secure vault")
        "encrypt_pickle_files": RemotePatchableInteger(ENCRYPT_PICKLE_FILES, "python_encrypt_pickle_files"),
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

    # Maximum replication factor which is used by client for uploading different files and tables (like cache, for example).
    # If `replication_factor` passed explicitly, this option will be ignored.
    # If |None|, replication factor will not be limited and default values will be used.
    # NB: this option can be overridden with settings from cluster.
    "max_replication_factor": RemotePatchableInteger(None, "max_replication_factor"),

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

    "query_tracker": {
        # Query state check interval.
        "poll_period": 1000,
        # Log level used for print stderr messages.
        "stderr_logging_level": "INFO",
        # Log level used for printing operation progress.
        "progress_logging_level": "INFO",
        # Abort operation when SIGINT is received while waiting for the operation to finish.
        "abort_on_sigint": True,
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
        "concatenate_size": 20,
        # This option allows to save intermediate data in remote_temp_files_directory / remote_temp_tables_directory.
        "use_tmp_dir_for_intermediate_data": True
    },

    # Size of block to read from response stream.
    "read_buffer_size": 8 * 1024 * 1024,

    # Do not fail while reading columns blocked by column acl
    "read_omit_inaccessible_columns": None,

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

    "started_by_command_length_limit": STARTED_BY_COMMAND_LENGTH_LIMIT,

    # Handle (some) type mismatch while reading, not initializing
    # i.e. do not throw exception while declare "weak" reading (f.e. read Optional[int] into int)
    # but throw excepion while reading bad data (if None really occurs)
    "runtime_type_validation": False,

    # Strawberry controller address.
    # May contain {stage}, {family} and {host_suffix} parameters.
    # NB: this option can be overridden with settings from cluster.
    "strawberry_ctl_address": RemotePatchableString("{stage}.{family}-ctl{host_suffix}", "strawberry_ctl_address"),
    # Cluster name under which the cluster is configured in the strawberry controller.
    # If |None|, the proxy url is used.
    # NB: this option can be overridden with settings from cluster.
    "strawberry_cluster_name": RemotePatchableString(None, "strawberry_cluster_name"),

    "upload_table_options": {
        # Only for tests.
        "write_arrow_batch_size": 64 * 1024,
    },

    "dump_table_options": {
        # When dumping a table, the size of the result row groups will exceed the set value.
        "min_batch_row_count": 0,
    },

    # if enabled, the password strength will be verified by the client when performing
    # a set_user_password request
    "enable_password_strength_validation": RemotePatchableBoolean(False, "python_enable_password_strength_validation"),
}

# pydoc :: default_config :: end


def transform_value(value, original_value):
    if original_value is False or original_value is True:
        if isinstance(value, str):
            raise TypeError("Value must be boolean instead of string")
    if isinstance(value, YsonEntity):
        return None
    return value


def get_default_config() -> DefaultConfigType:
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
    "YT_BASE_LAYER" : "operation_base_layer",
    "YT_HTTP_PROXY_ROLE": "proxy/http_proxy_role",
    "YT_RPC_PROXY_ROLE": "proxy/rpc_proxy_role",
}


SHORTCUTS = {
    "YT_PROXY": "proxy/url",
    "YT_PROXY_SUFFIX": "proxy/default_suffix",
    "YT_PROXY_URL_ALIASING_CONFIG": "proxy/aliases",
    "YT_HTTP_PROXY_ROLE": "proxy/http_proxy_role",
    "YT_RPC_PROXY_ROLE": "proxy/rpc_proxy_role",

    "YT_TOKEN": "token",
    "YT_TOKEN_PATH": "token_path",
    "YT_USE_TOKEN": "enable_token",
    "YT_CHECK_TOKEN": "check_token",
    "YT_ACCEPT_ENCODING": "proxy/accept_encoding",
    "YT_CONTENT_ENCODING": "proxy/content_encoding",
    "YT_FORCE_IPV4": "proxy/force_ipv4",
    "YT_FORCE_IPV6": "proxy/force_ipv6",

    "YT_VERSION": "api_version",

    "YT_DRIVER_CONFIG": "driver_config",
    "YT_DRIVER_CONFIG_PATH": "driver_config_path",

    "YT_USE_HOSTS": "proxy/enable_proxy_discovery",
    "YT_HOSTS": "proxy/proxy_discovery_url",

    "YT_MAX_REPLICATION_FACTOR": "max_replication_factor",

    "YT_ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE": "yamr_mode/always_set_executable_flag_on_files",
    "YT_USE_MAPREDUCE_STYLE_DESTINATION_FDS": "yamr_mode/use_yamr_style_destination_fds",
    "YT_TREAT_UNEXISTING_AS_EMPTY": "yamr_mode/treat_unexisting_as_empty",
    "YT_DELETE_EMPTY_TABLES": "yamr_mode/delete_empty_tables",
    "YT_USE_YAMR_SORT_REDUCE_COLUMNS": "yamr_mode/use_yamr_sort_reduce_columns",
    "YT_REPLACE_TABLES_WHILE_COPY_OR_MOVE": "yamr_mode/replace_tables_on_copy_and_move",
    "YT_CREATE_RECURSIVE": "yamr_mode/create_recursive",
    "YT_THROW_ON_EMPTY_DST_LIST": "yamr_mode/throw_on_missing_destination",
    "YT_RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED": "yamr_mode/run_map_reduce_if_source_is_not_sorted",
    "YT_USE_NON_STRICT_UPPER_KEY": "yamr_mode/use_non_strict_upper_key",
    "YT_CHECK_INPUT_FULLY_CONSUMED": "yamr_mode/check_input_fully_consumed",
    "YT_FORCE_DROP_DST": "yamr_mode/abort_transactions_with_remove",
    "YT_USE_YAMR_STYLE_PREFIX": "yamr_mode/use_yamr_style_prefix",

    "YT_OPERATION_STATE_UPDATE_PERIOD": "operation_tracker/poll_period",
    "YT_STDERR_LOGGING_LEVEL": "operation_tracker/stderr_logging_level",
    "YT_IGNORE_STDERR_IF_DOWNLOAD_FAILED": "operation_tracker/ignore_stderr_if_download_failed",
    "YT_KEYBOARD_ABORT": "operation_tracker/abort_on_sigint",

    "YT_FILE_STORAGE": "remote_temp_files_directory",
    "YT_LOCAL_TMP_DIR": "local_temp_directory",

    # Deprecated
    "YT_TEMP_TABLES_STORAGE": "remote_temp_tables_directory",
    # Non-deprecated version of TEMP_TABLES_STORAGE
    "YT_TEMP_DIR": "remote_temp_tables_directory",

    "YT_PREFIX": "prefix",

    "YT_POOL": "pool",
    "YT_MEMORY_LIMIT": "memory_limit",
    "YT_SPEC": "spec_defaults",
    "YT_BASE_LAYER": "operation_base_layer",
    "YT_TABLE_WRITER": "table_writer",

    "YT_RETRY_READ": "read_retries/enable",
    "YT_USE_RETRIES_DURING_WRITE": "write_retries/enable",
    "YT_USE_RETRIES_DURING_UPLOAD": "write_retries/enable",

    "YT_CHUNK_SIZE": "write_retries/chunk_size",

    "YT_DETACHED": "detached",

    "YT_TABULAR_DATA_FORMAT": "tabular_data_format",

    "YT_CONFIG_PATH": "config_path",
    "YT_CONFIG_FORMAT": "config_format",

    "YT_ARGCOMPLETE_VERBOSE": "argcomplete_verbose",

    "YT_USE_YAMR_DEFAULTS": "yamr_mode/use_yamr_defaults",
    "YT_IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST": "yamr_mode/ignore_empty_tables_in_mapreduce_list",

    "YT_CONFIG_PROFILE": "config_profile",

    "YT_ENCRYPT_PICKLE": "pickling/encrypt_pickle_files",
}


def update_config_from_env(
    config: VerifiedDict,
    config_profile: Optional[str] = None
) -> VerifiedDict:
    """Patch config from envs and the config file."""

    _update_from_env_patch(config)

    _update_from_file(config, config_profile=config_profile)

    _update_from_env_vars(config)

    return config


def _update_from_env_vars(
    config: VerifiedDict,
    shortcuts: Optional[dict] = None
):
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

    for key, value in os.environ.items():
        if not key.startswith("YT_"):
            continue

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


def _update_from_env_patch(config: VerifiedDict):
    if "YT_CONFIG_PATCHES" in os.environ:
        try:
            patches = yson._loads_from_native_str(os.environ["YT_CONFIG_PATCHES"],
                                                  yson_type="list_fragment",
                                                  always_create_attributes=False)
        except yson.YsonError as e:
            raise YtConfigError("Failed to parse YT config patches from 'YT_CONFIG_PATCHES' environment variable") from e

        try:
            for patch in reversed(list(patches)):
                common.update_inplace(config, patch)
        except Exception as e:  # noqa
            raise YtConfigError("Failed to apply config from 'YT_CONFIG_PATCHES' environment variable") from e


class ConfigParserV2:
    VERSION = 2
    _PROFILES_KEY = "profiles"
    _DEFAULT_PROFILE_KEY = "default_profile"

    def __init__(self, config: dict, profile: str):
        self._config = config
        self._profile = profile

    def extract(self):
        if self._PROFILES_KEY not in self._config:
            raise YtConfigError("Missing {0} key in YT config".format(self._PROFILES_KEY))
        profiles = self._config[self._PROFILES_KEY]
        if not isinstance(profiles, dict):
            raise YtConfigError("Profiles should be dict, not {0}".format(type(profiles)))
        current_profile = self._profile
        if current_profile is None:
            current_profile = self._config.get(self._DEFAULT_PROFILE_KEY)
            if current_profile is None:
                raise YtConfigError("Profile has not been set and there is no default profile in the config")
        if current_profile not in profiles:
            raise YtConfigError("Unknown profile {0}. Known profiles: {1}".format(
                current_profile,
                ",".join(profiles.keys())),
            )
        profile = profiles[current_profile]
        return profile


class _ConfigFSLoader:
    def __init__(self, current_path: Optional[str], user_path: Optional[str], global_path: Optional[str]):
        self._current_path = current_path
        self._user_path = user_path
        self._global_path = global_path

    def _is_file(self, path: str) -> bool:
        return os.path.isfile(path)

    def _is_readable(self, path: str) -> bool:
        try:
            with open(path, "r"):
                pass
            return True
        except IOError:
            pass
        return False

    def read_config(self) -> Tuple[Optional[bytes], Optional[str]]:
        path = self._get_config_path()
        if path is None:
            return None, None
        try:
            with open(path, "rb") as f:
                return f.read(), path
        except Exception as e:
            raise YtConfigError("Failed to read YT config from " + path) from e

    def _get_config_path(self) -> Optional[str]:
        if self._current_path is not None and self._is_file(self._current_path):
            return self._current_path

        config_path = self._global_path

        if self._user_path:
            if self._is_file(self._user_path):
                config_path = self._user_path
        if not self._is_readable(config_path):
            config_path = None
        return config_path


def _update_from_file(
    config: VerifiedDict,
    config_profile: Optional[str] = None,
    global_config_path: str = DEFAULT_GLOBAL_CONFIG_PATH,
    user_config_path: Optional[str] = None,
):
    if user_config_path is None:
        homedir = common.get_home_dir()
        if homedir is not None:
            user_config_path = os.path.join(homedir, DEFAULT_USER_CONFIG_REL_PATH)

    # These options should be processed before reading config file.
    for opt_name in ["YT_CONFIG_PATH", "YT_CONFIG_FORMAT", "YT_CONFIG_PROFILE"]:
        if opt_name in os.environ:
            config[SHORTCUTS[opt_name]] = os.environ[opt_name]

    if config_profile is None:
        config_profile = config["config_profile"]

    fs_helper = _ConfigFSLoader(
        current_path=config["config_path"],
        user_path=user_config_path,
        global_path=global_config_path,
    )
    content, path = fs_helper.read_config()
    if content is None:
        return

    config_format = config["config_format"]
    config_formats = {
        "yson": yson.loads,
        "json": json.loads,
    }
    load_func = config_formats.get(config_format)
    if load_func is None:
        raise common.YtError("Incorrect config_format '%s'" % config_format)

    try:
        parsed_data = load_func(content)
    except Exception as e:
        raise YtConfigError("Failed to parse YT config from " + path) from e

    config_version = parsed_data.get("config_version")
    if ConfigParserV2.VERSION == config_version:
        config_from_file = ConfigParserV2(config=parsed_data, profile=config_profile).extract()
    elif config_version is None:
        # Just a fallback to the plain format.
        # All keys are stored at the top level of the config.
        config_from_file = parsed_data
    else:
        raise common.YtError("Unknown config's version {0}".format(config_version))

    common.update_inplace(config, config_from_file)


def get_config_from_env(
    config_profile: Optional[str] = None
) -> VerifiedDict:
    """Get default config with patches from envs"""
    return update_config_from_env(get_default_config(), config_profile=config_profile)


def _get_settings_from_cluster_callback(
    config: Optional[VerifiedDict] = None,
    client=None
):
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
