PY3_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
)

PY_SRCS(
    publisher/__init__.py
    publisher/build_downloader.py
    publisher/config_generator.py
    publisher/local_manager.py
    publisher/publish_cluster.py
    publisher/remote_manager.py
    publisher/utils.py
)

END()
