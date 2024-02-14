PY3_LIBRARY()

PY_SRCS(
    TOP_LEVEL
    yt_dashboards/__init__.py
    yt_dashboards/artemis.py
    yt_dashboards/bundle_ui_dashboard.py
    yt_dashboards/cache.py
    yt_dashboards/chyt.py
    yt_dashboards/cluster_resources.py
    yt_dashboards/key_filter.py
    yt_dashboards/lsm.py
    yt_dashboards/master.py
    yt_dashboards/scheduler_internal.py
    yt_dashboards/scheduler_pool.py
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
