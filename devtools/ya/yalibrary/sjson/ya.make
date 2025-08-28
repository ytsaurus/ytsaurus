PY23_LIBRARY()

PY_SRCS(
    __init__.py
    dumper.pyx
    loader.pyx
)

SRCS(
    lib/encoder.cpp
    lib/dump.cpp
    lib/load.cpp
    lib/filter.cpp
)

PEERDIR(
    contrib/python/six
    devtools/libs/json_sax
    library/cpp/pybind
)

END()

RECURSE_FOR_TESTS(
    fat
    tests
)
