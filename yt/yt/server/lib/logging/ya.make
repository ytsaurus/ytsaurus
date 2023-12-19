LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    category_registry.cpp
    program_describe_structured_logs_mixin.cpp
)

PEERDIR(
    library/cpp/yt/threading

    yt/yt/client
    yt/yt/core
    yt/yt/library/program
)

END()

RECURSE_FOR_TESTS(
  unittests
)
