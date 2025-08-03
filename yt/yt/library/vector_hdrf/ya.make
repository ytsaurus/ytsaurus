LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    piecewise_linear_function_helpers.cpp
    # Files below this line, depends on core/
    job_resources.cpp
    resource_vector.cpp
    resource_volume.cpp
    fair_share_update.cpp
    base_element.cpp
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
