LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    common.cpp
    describe_computation.cpp
    describe_computations.cpp
    describe_partition.cpp
    describe_pipeline.cpp
    describe_worker.cpp
    describe_workers.cpp
    draw_pipeline_graph.cpp
    fill_graph_limits.cpp
    graph_entity_id.cpp
    intermediate_description.cpp
    pipeline_description_unroll.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/companion
    yt/yt/flow/library/cpp/misc
)

IF (NOT OPENSOURCE)
    PEERDIR(
        yt/yt/flow/yandex/library/cpp/internal_urls
    )
ENDIF()

END()

RECURSE_FOR_TESTS(unittests)
