LIBRARY()

SRCS(
    bind.cpp
)

PEERDIR(
    yt/cpp/roren/interface
    yt/cpp/roren/library/construction_serializable
    yt/cpp/roren/library/save_load_wrapper
)

END()

RECURSE_FOR_TESTS(ut)
