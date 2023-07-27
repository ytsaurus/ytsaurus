
PY2_PROGRAM(transfer-manager)

OWNER(
    g:yt g:yt-python
)

PEERDIR(
    yt/python/yt/transfer_manager/client
)

COPY_FILE(yt/python/yt/transfer_manager/client/bin/transfer-manager __main__.py)

PY_SRCS(__main__.py)

END()
