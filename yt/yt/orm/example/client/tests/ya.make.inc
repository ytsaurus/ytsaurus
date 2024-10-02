SET(ROOT_DIR yt/yt/orm/example/client/tests)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/ya_cpp.make.inc)

SET(COMMON_FILES
    ${ROOT_DIR}/common.cpp
    ${ROOT_DIR}/test_common.cpp
    ${ROOT_DIR}/test_generic_orm_client.cpp
    ${ROOT_DIR}/test_object_traits.cpp
    ${ROOT_DIR}/test_semaphore_set.cpp
)

SET(RPC_PROXY_CONNECTION_SRCS
    ${COMMON_FILES}
    ${ROOT_DIR}/test_non_owned_attached_transaction.cpp
)

SET(RPC_PROXY_CONNECTION_CLUSTER_SRCS
    ${RPC_PROXY_CONNECTION_SRCS}
    ${ROOT_DIR}/test_cluster.cpp
)

SET(RPC_PROXY_CONNECTION_SHUTDOWN_SRCS
    ${ROOT_DIR}/common.cpp
    ${ROOT_DIR}/test_shutdown.cpp
)

SET(NATIVE_CONNECTION_SRCS
    ${COMMON_FILES}
    ${ROOT_DIR}/test_rpc_proxy_collocation.cpp
)

SET(NATIVE_CONNECTION_CLUSTER_SRCS
    ${COMMON_FILES}
    ${ROOT_DIR}/test_cluster.cpp
)

SET(NATIVE_CONNECTION_SHUTDOWN_SRCS
    ${ROOT_DIR}/common.cpp
    ${ROOT_DIR}/test_shutdown.cpp
)

ENV(EXAMPLE_MASTER_COUNT=${EXAMPLE_MASTER_COUNT})

PEERDIR(
    yt/yt/orm/example/client/native

    yt/yt/orm/example/client/misc

    yt/yt/orm/client/generic
    yt/yt/orm/client/native

    yt/yt/orm/server

    yt/yt/client

    yt/yt/core
    yt/yt/core/rpc/grpc

    yt/yt/core/test_framework

    library/cpp/testing/gtest_protobuf
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_make_integration.inc)
INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/python/ya_programs.make.inc)
DEPENDS(
    ${ARCADIA_ROOT}/yt/yt/orm/example/python/recipe/bin
)
