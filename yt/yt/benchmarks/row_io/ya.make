G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
    pipeline.cpp
)

PEERDIR(
    yt/yt/benchmarks/row_io/dataset
    yt/yt/ytlib
    yt/yt/library/formats
)

SIZE(MEDIUM)

END()

RECURSE(
    dataset
    import_table
)
