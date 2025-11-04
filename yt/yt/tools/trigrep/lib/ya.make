LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    uncompressed_reader.cpp
    zstd_reader.cpp
    index_builder.cpp
    line_reader.cpp
    matcher.cpp
    helpers.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/containers/absl_flat_hash
    library/cpp/yt/coding
    contrib/libs/zstd
)

END()
