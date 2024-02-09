PY3_PROGRAM(generate_dashboards)

PY_SRCS(
    MAIN main.py
)

PEERDIR(
    yt/admin/dashboards
)

IF (NOT OPENSOURCE)
    PEERDIR(
        library/python/resource

        # Required for monitoring backend.
        solomon/protos/api/v3
    )
    RESOURCE(
        yt/admin/dashboards/yt_dashboards/bin/config.json /yt_dashboards/config.json
    )
ENDIF()

END()
