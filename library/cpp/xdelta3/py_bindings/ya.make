PY23_LIBRARY()

PY_REGISTER(yt.xdelta_aggregate_column.bindings)

IF (PYTHON3)
    # causes warnings in pycxx; patch pycxx instead?
    NO_COMPILER_WARNINGS()
ENDIF()

SRCS(
    main.cpp
    py_state.cpp
)

PEERDIR(
    library/cpp/xdelta3/state

    library/cpp/pybind

    contrib/python/py3c
)

END()

RECURSE(
    test
)
