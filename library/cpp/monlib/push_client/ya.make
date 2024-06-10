LIBRARY()

SRCS(
    pull_client.cpp
    push_client.cpp
    auth.cpp
    builder.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/monlib/deprecated/json
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/spack
    library/cpp/monlib/metrics
    library/cpp/retry
    library/cpp/string_utils/url
)

END()

RECURSE(
    tvm
)

RECURSE_FOR_TESTS(ut)
