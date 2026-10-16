PY3_LIBRARY()

PY_SRCS(
    garbage_collector.py
    logger.py
    object_counts.py
)

PEERDIR(
    yt/cron/library

    yt/yt/orm/python/orm/library
)

END()
