PEERDIR(
    mapreduce/yt/python
)

DEPENDS(
    # For yt_thor.
    yt/packages/19_4
    # For yt_local binary.
    yt/python/yt/local/bin/yt_local_make
)

INCLUDE(${ARCADIA_ROOT}/yt/python/yt/environment/ya_programs.make.inc)

