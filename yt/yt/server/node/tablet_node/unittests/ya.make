GTEST(unittester-tablet-node)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    proto/simple_transaction_supervisor.proto

    lookup_ut.cpp
    ordered_dynamic_store_ut.cpp
    ordered_store_manager_ut.cpp
    overload_controller_ut.cpp
    simple_tablet_manager.cpp
    simple_transaction_supervisor.cpp
    sorted_chunk_store_ut.cpp
    sorted_dynamic_store_pt.cpp
    sorted_dynamic_store_ut.cpp
    sorted_store_manager_stress.cpp
    sorted_store_manager_ut.cpp
    tablet_cell_write_manager_ut.cpp
    tablet_context_mock.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/library/query/row_comparer
    yt/yt/server/node
    yt/yt/server/tools
    yt/yt/server/lib/hydra_common/mock
)

IF (AUTOCHECK OR SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(
        ya:fat
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_TESTS()

SPLIT_FACTOR(8)

REQUIREMENTS(ram:20)

END()
