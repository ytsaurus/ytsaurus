LIBRARY()

OWNER(evelkin)

PEERDIR(
    library/cpp/pybind/example
)

PYTHON2_ADDINCL()

PY_REGISTER(pybindexample)

SRCS(
    main.cpp
)

END()
