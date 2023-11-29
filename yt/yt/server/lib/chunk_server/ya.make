LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    immutable_chunk_meta.cpp
    helpers.cpp

    proto/job_common.proto
    proto/job_tracker_service.proto
    proto/job.proto
)

PEERDIR(
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    unittests
)
