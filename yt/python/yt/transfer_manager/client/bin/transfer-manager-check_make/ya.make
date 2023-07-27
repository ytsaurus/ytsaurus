
PY2_PROGRAM(transfer-manager-check)

OWNER(
    g:yt g:yt-python
)

PEERDIR(
    yt/python/yt/transfer_manager/client
)

COPY_FILE(yt/python/yt/transfer_manager/client/bin/transfer-manager-check __main__.py)

PY_SRCS(__main__.py)

END()
