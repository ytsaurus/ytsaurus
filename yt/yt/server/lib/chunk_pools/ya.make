LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chunk_pool.cpp
    chunk_pool_output_merger.cpp
    config.cpp
    helpers.cpp
    input_chunk_mapping.cpp
    input_stream.cpp
    job_size_adjuster.cpp
    job_size_tracker.cpp
    legacy_job_manager.cpp
    legacy_sorted_chunk_pool.cpp
    legacy_sorted_job_builder.cpp
    multi_chunk_pool.cpp
    new_job_manager.cpp
    new_sorted_chunk_pool.cpp
    new_sorted_job_builder.cpp
    ordered_chunk_pool.cpp
    resource.cpp
    shuffle_chunk_pool.cpp
    sorted_chunk_pool.cpp
    sorted_job_builder.cpp
    sorted_staging_area.cpp
    unordered_chunk_pool.cpp
    vanilla_chunk_pool.cpp
)

# Do not drop the unused weak function CreateChunkPool().
GLOBAL_SRCS(
    chunk_pool_factory.cpp
)

PEERDIR(
    yt/yt/ytlib

    # TODO(max42): eliminate.
    yt/yt/server/lib/controller_agent
)

END()

RECURSE_FOR_TESTS(
    unittests
)
