PY3_LIBRARY()

PY_SRCS(
    TOP_LEVEL
    yt_dashboards/__init__.py
    yt_dashboards/artemis.py
    yt_dashboards/bundle_ui/bundle_ui_dashboard.py
    yt_dashboards/bundle_ui/__init__.py
    yt_dashboards/bundle_ui/user_load.py
    yt_dashboards/bundle_ui/lsm.py
    yt_dashboards/bundle_ui/cpu.py
    yt_dashboards/bundle_ui/disk.py
    yt_dashboards/bundle_ui/common.py
    yt_dashboards/bundle_ui/maintenance.py
    yt_dashboards/bundle_ui/memory.py
    yt_dashboards/bundle_ui/network.py
    yt_dashboards/bundle_ui/efficiency.py
    yt_dashboards/bundle_ui/resources.py
    yt_dashboards/bundle_ui/proxy_resources.py
    yt_dashboards/bundle_ui/proxy_details.py
    yt_dashboards/bundle_ui/planning.py
    yt_dashboards/bundle_ui/key_filter.py
    yt_dashboards/cache.py
    yt_dashboards/chyt.py
    yt_dashboards/cluster_resources.py
    yt_dashboards/flow.py
    yt_dashboards/key_filter.py
    yt_dashboards/lsm.py
    yt_dashboards/master.py
    yt_dashboards/scheduler_internal.py
    yt_dashboards/scheduler_pool.py
    yt_dashboards/exe_nodes.py
    yt_dashboards/data_nodes.py
    yt_dashboards/queue_and_consumer_metrics.py
    yt_dashboards/common/__init__.py
    yt_dashboards/common/postprocessors.py
    yt_dashboards/common/runner.py
    yt_dashboards/common/sensors.py
)

IF (OPENSOURCE)
    PY_SRCS(
        TOP_LEVEL
        yt_dashboards/common/opensource_settings.py
    )
ELSE()
    PY_SRCS(
        TOP_LEVEL
        yt_dashboards/common/settings.py
        yt_dashboards/constants.py
    )
ENDIF()

PEERDIR(
    yt/admin/dashboard_generator
)

END()

RECURSE(
    yt_dashboards/bin
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        yt_dashboards/tests
    )
ENDIF()
