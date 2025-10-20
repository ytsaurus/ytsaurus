LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cgroup.cpp
    cgroups_new.cpp
    config.cpp
    container_devices_checker.cpp
    helpers.cpp
    instance.cpp
    instance_limits_tracker.cpp
    process.cpp
    porto_executor.cpp
    porto_resource_tracker.cpp
    porto_health_checker.cpp
)

PEERDIR(
    library/cpp/porto/proto

    yt/yt/library/process

    yt/yt/core
)

IF(OS_LINUX)
    PEERDIR(
        library/cpp/porto
    )
ENDIF()

END()

RECURSE(
    cri
)

RECURSE_FOR_TESTS(
    unittests
)
