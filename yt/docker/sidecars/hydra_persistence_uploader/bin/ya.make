PY3_PROGRAM(hydra_persistence_uploader)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/docker/sidecars/hydra_persistence_uploader/lib
    yt/python/client
)

END()

