LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_entrypoint.cpp
    companion_manager.cpp
    companion_process_manager.cpp
    java_companion_manager.cpp
    java_process_manager.cpp
    jvm_options.cpp
    process_manager_base.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/client

    yt/yt/library/process

    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/resources
)

END()

RECURSE_FOR_TESTS(
    unittests
)
