PY3_PROGRAM(oauth_health)

PEERDIR(
    yt/odin/checks/lib/quorum_health
    yt/odin/checks/lib/check_runner

)

PY_SRCS(
    __main__.py
)

END()
