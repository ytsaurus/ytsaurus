LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap_dataset.cpp
    config.cpp
    dataset.cpp
    dataset_operation.cpp
    decorate_dataset.cpp
    map_dataset.cpp
    operation.cpp

    operation/map.cpp
    operation/multi_map.cpp
    operation/reduce.cpp
    operation/util.cpp

    reduce_dataset.cpp
    run.cpp
    runner.cpp
    sort_dataset.cpp
    table.cpp
    table_dataset.cpp
    test_home.cpp
    test_program.cpp
    util.cpp
)

PEERDIR(
    library/cpp/getopt

    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/io

    yt/systest/proto

    yt/yt/client

    yt/yt/library/containers
    yt/yt/library/program
    yt/yt/library/profiling/solomon

    yt/yt_proto/yt/client
)

END()

RECURSE(
    bin
    helm
    scripts
    unittests
)
