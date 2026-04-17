LIBRARY()

PEERDIR(
    library/cpp/dwarf_backtrace
    library/cpp/getopt
    yt/cpp/mapreduce/interface
    yt/cpp/roren/interface
)

SRCS(
    messages.proto
    metrics.cpp
    pipeline.cpp
    id_to_path_updater.cpp
)

END()
