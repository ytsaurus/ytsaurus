PY23_LIBRARY()

OWNER(g:yt g:yt-python)

PEERDIR(
    mapreduce/yt/client
)

PY_SRCS(
    __init__.py
    cpp_wrapper.pyx
)

END()

