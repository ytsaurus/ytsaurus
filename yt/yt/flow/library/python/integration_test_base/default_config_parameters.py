import mergedeep
import os
import yatest

IS_CI = bool(os.getenv('CI'))

FLOW_NODE_DEFAULT_CONFIG = {
    "solomon_exporter": {
        "enable_self_profiling": False,
    },
    "controller": {
        "warm_up_time": 0,
        "scheduler_period": "100ms",
        "cache_period": "100ms",
        "feedback_period": "100ms",
        "publish_retry_period": "100ms",
        "lease_manager": {
            "lease_timeout": "15s",
            "lease_ping_period": "1s",
            "lease_check_period": "1s",
        },
        "controller_service": {
            "set_spec_retry_period": "100ms",
        },
    },
    "logging": {
        "suppressed_messages": [],
        "rules": [
            {
                "exclude_categories": [
                    "BufferMetrics",
                    "Bus",
                    "Concurrency",
                    "Dns",
                    "FlowAuthenticator",
                    "Jaeger",
                    "Monitoring",
                    "Net",
                    "Profiling",
                    "QueryClient",
                    "RpcClient",
                    "RpcProxyClient",
                    "RpcServer",
                    "Solomon",
                ],
                "min_level": "info" if IS_CI else "debug",
                "writers": ["FilteredDebug"],
            },
            {
                "min_level": "error",
                "writers": ["AllError"],
            },
            {"include_categories": ["PublicFlowController"], "min_level": "debug", "writers": ["ControllerLogWriter"]},
        ],
        "writers": {
            "FilteredDebug": {
                "type": "file",
                "file_name": "to_be_overwritten_by_test_framework",
            },
            "AllError": {
                "type": "file",
                "file_name": "to_be_overwritten_by_test_framework",
            },
        },
    },
    "error_backtrace_enricher": {
        "level": yatest.common.get_param("ERROR_BACKTRACE_ENRICHER_LEVEL", "enabled_for_not_native_errors"),
    },
    "tcmalloc": {
        # Sample GWP-ASAN more aggressively than in production (16 MiB) to catch heap corruption in tests.
        "guarded_sampling_rate": 2097152,  # 2 MiB.
    },
}


FLOW_RUNNER_DEFAULT_CONFIG = {
    "abort_on_unrecognized_options": True,
    "abort_on_specs_parseability_error": True,
}


# Note, that this default doesn't cover map fields such as `computations`.
DEFAULT_DYNAMIC_SPEC = {
    "job_manager": {
        "lost_job_timeout": "5s",
        "minimum_worker_count": 1,
        "faulty_address_attempts": 1000000,
        # Disable CPU-aware balancer tests
        "use_cpu_aware_balancer": False,
    },
    "message_distributor": {
        "push_messages_timeout": "1s",
    },
    "controller_connector": {
        "controller_discover_period": "500ms",
        "controller_heartbeat_period": "300ms",
        "controller_heartbeat_rpc_timeout": "1s",
        "controller_handshake_rpc_timeout": "1s",
        "orchid_update_period": "500ms",
    },
}

COMPUTATION_DEFAULT_DYNAMIC_SPEC = {
    "batch_duration": "500ms",
    "retryable_client": {
        "min_inner_timeout": "1500ms",
        "timeout": "20s",
        "backoff": {
            "invocation_count": 5,
            "min_backoff": "1500ms",
        },
    },
    "output_store": {
        # Small chunk size makes tests exercise chunk rollover even on tiny inputs.
        "max_chunk_message_count": 23,
        # Cheaper to compress than the prod default (zstd_6); tests don't care about stored size.
        "compression_codec": "lz4",
    },
}


def _put_default_under_config(*, config: dict, default: dict) -> None:
    override = dict(config)
    config.clear()
    mergedeep.merge(config, default, override)


def fill_flow_node_test_defaults(config):
    _put_default_under_config(config=config, default=FLOW_NODE_DEFAULT_CONFIG)


def fill_flow_spec_test_defaults(spec, dynamic_spec):
    _put_default_under_config(config=dynamic_spec, default=DEFAULT_DYNAMIC_SPEC)
    for computation_name, computation_spec in spec["computations"].items():
        # Exercise the compact output store path by default in tests.
        computation_spec.setdefault("use_compact_partition_output", True)
        computation_dynamic_spec = dynamic_spec.setdefault("computations", {}).setdefault(computation_name, {})
        _put_default_under_config(config=computation_dynamic_spec, default=COMPUTATION_DEFAULT_DYNAMIC_SPEC)


def fill_runner_test_defaults(config):
    _put_default_under_config(config=config, default=FLOW_RUNNER_DEFAULT_CONFIG)
    fill_flow_spec_test_defaults(config["spec"], config.setdefault("dynamic_spec", {}))
