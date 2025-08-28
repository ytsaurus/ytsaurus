LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    piecewise_linear_function_helpers.cpp

    # Files below this line, depends on core/
    base_element.cpp
    fair_share_update.cpp
    job_resources.cpp
    resource_vector.cpp
    resource_volume.cpp
    serialize.cpp
)

PEERDIR(
    yt/yt/library/numeric

    # Core dependencies.
    yt/yt/library/numeric/serialize
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
