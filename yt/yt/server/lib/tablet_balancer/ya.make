LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    balancing_helpers.cpp
    config.cpp
    parameterized_balancing_helpers.cpp
    table.cpp
    tablet.cpp
    tablet_cell.cpp
    tablet_cell_bundle.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/orm/library/query
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    dry_run
    unittests
)
