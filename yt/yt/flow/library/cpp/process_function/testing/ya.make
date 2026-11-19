LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

PEERDIR(
    yt/yt/flow/library/cpp/process_function/testing/no_engine
    yt/yt/library/query/engine
)

END()

RECURSE(
    no_engine
)
