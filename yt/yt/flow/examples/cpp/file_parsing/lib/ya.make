LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_parsing_functions.cpp
    messages.cpp
    parse_executor.cpp
    parse_executor_resource.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/resources
)

END()
