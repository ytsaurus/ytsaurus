import os

from google.protobuf import text_format

from yql.cfg.proto import worker_config_pb2
from yql.essentials.core.file_storage.proto import file_storage_pb2
from yql.essentials.providers.common.proto import gateways_config_pb2


_VALID_LOG_LEVELS = frozenset({"FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"})

# Matches NDqs::ENodeIdLimits::MinWorkerNodeId in interconnect_helpers.h
DQ_MIN_WORKER_NODE_ID = 512

# YT TUserJobSpec defaults when memory_limit/cpu_limit are omitted from operation spec
# (yt/yt/ytlib/scheduler/config.cpp): 512 MB and 1 CPU — too small for dq_vanilla_job.
# Local clique uses the same values (dq_create_clique.sh).
DQ_DEFAULT_MEMORY_LIMIT = 8 * 1024 ** 3  # 8589934592
DQ_DEFAULT_CPU_LIMIT = 2


def _normalize_log_level(level):
    normalized = (level or "DEBUG").upper()
    if normalized not in _VALID_LOG_LEVELS:
        raise ValueError(
            "Invalid qtworker log level: {0!r}. Expected one of: {1}".format(
                level,
                ", ".join(sorted(_VALID_LOG_LEVELS)),
            )
        )
    return normalized


def _add_settings(destination, settings):
    for name, value in settings:
        setting = destination.add()
        setting.Name = name
        setting.Value = value


def _enum_value(message, enum_name, value_name):
    return message.DESCRIPTOR.enum_types_by_name[enum_name].values_by_name[value_name].number


def build_gateways_conf(
    *,
    cluster_name,
    cluster_address,
    mr_job_bin,
    yt_debug_log_file,
    enable_dq=False,
    remote_clusters=(),
):
    config = gateways_config_pb2.TGatewaysConfig()

    yt_config = config.Yt
    yt_config.GatewayThreads = 0
    yt_config.YtLogLevel = gateways_config_pb2.YL_INFO
    yt_config.MrJobBin = mr_job_bin
    yt_config.YtDebugLogFile = yt_debug_log_file
    _add_settings(yt_config.DefaultSettings, [
        ("DefaultMaxJobFails", "1"),
        ("LocalCalcLimit", "16"),
        ("ReleaseTempData", "immediate"),
        ("NativeYtTypeCompatibility", "production"),
        ("UseYqlRowSpecCompactForm", "true"),
        ("UseNewPredicateExtraction", "true"),
        ("PruneKeyFilterLambda", "true"),
        ("_UseKeyBoundApi", "true"),
        ("_EnableDynamicTablesWrite", "true"),
        ("JoinCommonUseMapMultiOut", "true"),
        ("UseAggPhases", "true"),
        ("_EnableWriteReorder", "true"),
        ("_EnableYtPartitioning", "true"),
        ("UseRPCReaderInDq", "true"),
        ("PartitionByConstantKeysViaMap", "true"),
        ("ColumnGroupMode", "perusage"),
        ("TableContentLocalExecution", "10M"),
        ("UseIntermediateStreams", "true"),
        ("RuntimeClusterSelection", "auto"),
        ("_EnableRLSTablesSupport", "true"),
        ("_MinJobStateSizeToPassViaFile", "32K"),
        ("_EnableQLFilter", "true"),
        ("_EnforceRegexpProbabilityFail", "100"),
    ])

    remote_file_pattern = yt_config.RemoteFilePatterns.add()
    remote_file_pattern.Pattern = r"yt://([a-zA-Z0-9\-_]+)/([^&@?]+)$"
    remote_file_pattern.Cluster = "$1"
    remote_file_pattern.Path = "$2"

    cluster_mapping = yt_config.ClusterMapping.add()
    cluster_mapping.Name = cluster_name
    cluster_mapping.Cluster = cluster_address
    cluster_mapping.Default = True
    _add_settings(cluster_mapping.Settings, [("_AllowRemoteClusterInput", "true")])

    for name, address in remote_clusters:
        cluster_mapping = yt_config.ClusterMapping.add()
        cluster_mapping.Name = name
        cluster_mapping.Cluster = address

    if enable_dq:
        config.Dq.DefaultAutoPercentage = 100
        _add_settings(config.Dq.DefaultSettings, [
            ("EnableComputeActor", "1"),
            ("MemoryLimit", "3G"),
        ])

    for name in [
        "AnsiOrderByLimitInUnionAll",
        "DisableCoalesceJoinKeysOnQualifiedAll",
        "AnsiRankForNullableKeys",
        "DisableAnsiOptionalAs",
        "FlexibleTypes",
        "CompactNamedExprs",
        "Antlr4",
        "DistinctOverWindow",
        "UnorderedResult",
    ]:
        config.SqlCore.ExtendedTranslationFlags.add().Name = name

    for name in [
        "DisableYsonCastToString",
        "_EnableMatchRecognize",
        "_EnableStreamLookupJoin",
        "EnableLineage",
        "UseUrlListerForFolder",
        "UseCanonicalLibrarySuffix",
    ]:
        config.YqlCore.Flags.add().Name = name
    constraint_check = config.YqlCore.Flags.add()
    constraint_check.Name = "EnableConstraintCheck"
    constraint_check.Args.extend(["Distinct", "Unique"])

    config.Ytflow.GatewayThreads = 1
    _add_settings(config.Ytflow.DefaultSettings, [
        ("GracefulUpdate", "1"),
        ("FiniteStreams", "1"),
        ("ControllerCount", "1"),
        ("ControllerMemoryLimit", "1G"),
        ("WorkerCount", "1"),
        ("WorkerMemoryLimit", "1G"),
        ("YtPartitionCount", "1"),
    ])

    return text_format.MessageToString(config)


def build_fs_conf(*, filecache_path):
    config = file_storage_pb2.TFileStorageConfig()
    config.Path = filecache_path
    config.MaxFiles = 1000
    config.MaxSizeMb = 512
    config.Threads = 2
    config.RetryCount = 3
    return text_format.MessageToString(config)


def _configure_worker_dq(
    dq_config,
    *,
    cluster_name,
    cluster_address,
    token_path,
    user,
    dq_vanilla_job,
    dq_vanilla_job_lite,
    dq_yt_prefix,
    dq_coordinator_prefix,
    dq_port_start,
    dq_port_finish,
    dq_worker_actor_start_port,
    dq_worker_same_actor_ports,
    max_jobs=1,
    jobs_per_operation=1,
    min_node_id=DQ_MIN_WORKER_NODE_ID,
    memory_limit=DQ_DEFAULT_MEMORY_LIMIT,
    cpu_limit=DQ_DEFAULT_CPU_LIMIT,
):
    # MinNodeId is inclusive, MaxNodeId is exclusive. MaxJobs caps concurrent workers
    # (JobsPerOperation only groups them into YT operations).
    max_node_id = min_node_id + max_jobs

    dq_config.User = user
    dq_config.Group = user
    dq_config.EnableClique = True
    dq_config.PortStart = dq_port_start
    dq_config.PortFinish = dq_port_finish

    backend = dq_config.YtBackends.add()
    backend.ClusterName = cluster_name
    backend.ProxyAddress = cluster_address
    backend.JobsPerOperation = jobs_per_operation
    backend.MaxJobs = max_jobs
    backend.MinNodeId = min_node_id
    backend.MaxNodeId = max_node_id
    backend.MemoryLimit = memory_limit
    backend.CpuLimit = cpu_limit
    backend.VanillaJobLite = dq_vanilla_job_lite
    backend.VanillaJobCommand = "./dq_vanilla_job"
    vanilla_job_file = backend.VanillaJobFile.add()
    vanilla_job_file.Name = "dq_vanilla_job"
    vanilla_job_file.LocalPath = dq_vanilla_job
    backend.Prefix = dq_yt_prefix
    backend.UploadPrefix = dq_yt_prefix
    backend.TokenFile = token_path
    backend.User = user
    backend.UseTmpFs = False
    backend.ActorStartPort = dq_worker_actor_start_port
    backend.SameActorPorts = dq_worker_same_actor_ports
    backend.CanUseComputeActor = True

    coordinator = dq_config.YtCoordinator
    coordinator.ClusterName = cluster_name
    coordinator.ProxyAddress = cluster_address
    coordinator.Prefix = dq_coordinator_prefix
    coordinator.TokenFile = token_path
    coordinator.User = user

    dq_config.Control.Disabled = True


def build_worker_conf(
    *,
    instance_root,
    udfs_dir,
    udf_resolver_path,
    udf_dep_stub_path,
    token_path,
    qtworker_log_file,
    qt_socket_dir,
    inspector_port,
    core_port,
    core_task_port,
    dq_interconnect_port=None,
    dq_grpc_port=None,
    enable_dq=False,
    dq_params=None,
    log_level="DEBUG",
    token_resolver_socket_path=None,
):
    if enable_dq and dq_params is None:
        raise ValueError("dq_params is required when enable_dq is true")
    if enable_dq and (dq_interconnect_port is None or dq_grpc_port is None):
        raise ValueError("dq_interconnect_port and dq_grpc_port are required when enable_dq is true")

    log_level = _normalize_log_level(log_level)
    config = worker_config_pb2.TWorkerConfig()
    config.ProcessMode = worker_config_pb2.MULTI_PROCESS
    config.User = "yql"
    config.Group = "yql"
    config.CoreUser = "yql"
    config.CoreGroup = "yql"

    qt_config = config.Qt
    qt_config.SocketDir = qt_socket_dir
    qt_config.CorePort = core_port
    qt_config.CoreTaskPort = core_task_port
    if enable_dq:
        qt_config.DqInterconnectPort = dq_interconnect_port
        qt_config.DqGrpcPort = dq_grpc_port

    logging_config = config.Logging
    file_destination = logging_config.LogDest.add()
    file_destination.Type = _enum_value(logging_config, "ELogTo", "FILE")
    file_destination.Target = qtworker_log_file
    logging_config.LogDest.add().Type = _enum_value(logging_config, "ELogTo", "STDERR")
    level = _enum_value(logging_config, "ELevel", log_level)
    logging_config.AllComponentsLevel = level
    for component_name in ["CORE", "CORE_EXECUTION"]:
        component = logging_config.Levels.add()
        component.C = _enum_value(logging_config, "EComponent", component_name)
        component.L = level
    logging_config.Format.IsStrict = False

    inspector = config.Inspector
    inspector.Hosts.append("127.0.0.1")
    inspector.Port = inspector_port
    inspector.MaxInFlight = 7000
    inspector.SendTimeoutSeconds = 60
    inspector.TotalTimeoutSeconds = 120
    inspector.MaxSendRetries = 60
    inspector.RetryIntervalMillis = 1000

    server = config.Server
    server.PortStart = 23000
    server.PortFinish = 24000
    server.Threads = 3
    server.MaxInFlight = 7000
    server.SendTimeoutSeconds = 60
    server.TotalTimeoutSeconds = 120

    executor = config.Executor
    executor.Threads = 0
    executor.QueueMaxSize = 1000
    executor.QueuePerUserMaxSize = 50
    executor.LoadUdfsDir = udfs_dir
    executor.UdfResolverPath = udf_resolver_path
    executor.UdfResolverFilterSyscalls = True
    executor.UdfDependencyStubPath = udf_dep_stub_path
    executor.LogLimit = "50M"
    executor.TaskProcessTrackDir = os.path.join(instance_root, "qtworker-tasks")
    executor.RunDir = os.path.join(instance_root, "run")
    credential = executor.DefaultCredentials.add()
    credential.Alias = "default_local"
    credential.Category = "custom"
    credential.Path = token_path
    executor.MemoryLimitMb = 51200
    if token_resolver_socket_path is not None:
        executor.YtTokenResolver.YqlAgent.UnixSocketPath = token_resolver_socket_path

    if enable_dq:
        _configure_worker_dq(config.Dq, **dq_params)

    config.DisableUnwinding = True
    return text_format.MessageToString(config)
