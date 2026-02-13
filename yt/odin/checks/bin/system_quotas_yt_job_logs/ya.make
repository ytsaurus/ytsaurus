PY3_PROGRAM(system_quotas_yt_job_logs)

PEERDIR(
    yt/odin/checks/lib/system_quotas
    yt/odin/checks/lib/check_runner
)

IF (NOT OPENSOURCE)
    PEERDIR(yt/odin/checks/lib/yandex_helpers)
ENDIF()


PY_SRCS(
    __main__.py
)

END()
