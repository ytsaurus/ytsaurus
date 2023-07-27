LIBRARY()

OWNER(evelkin)

PEERDIR(
    library/cpp/pybind
    util
)

PYTHON2_ADDINCL()

SRCS(
    v2_example.cpp
)

END()
