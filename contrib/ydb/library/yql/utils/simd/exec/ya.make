EXECTEST()

RUN(
    stream_store
)

RUN(
    pack_tuple
)

RUN(
    tuples_to_bucket
)

DEPENDS(
    contrib/ydb/library/yql/utils/simd/exec/stream_store
    contrib/ydb/library/yql/utils/simd/exec/pack_tuple
    contrib/ydb/library/yql/utils/simd/exec/tuples_to_bucket
)

PEERDIR(
    contrib/ydb/library/yql/utils/simd
)

END()

RECURSE(
    pack_tuple
    tuples_to_bucket
    stream_store
)