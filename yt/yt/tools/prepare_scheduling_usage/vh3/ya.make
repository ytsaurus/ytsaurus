PY3_PROGRAM()

OWNER(ignat)

PY_MAIN(vh3.cli:main)

PEERDIR(
    nirvana/vh3/src
)

PY_SRCS(
    prepare_scheduling_usage_log.py
)

END()
