LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    bigrt/lib/consuming_system/config
    bigrt/lib/processing/shard_processor/fallback/config
    bigrt/lib/processing/shard_processor/stateless/config
    bigrt/lib/writer/swift/config
    bigrt/lib/writer/yt_queue/proto
    yt/cpp/roren/library/config_extension_mixin
    yt/yt/core
)

END()
