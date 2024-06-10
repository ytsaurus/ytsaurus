PROTO_LIBRARY()

SRCS(
    config.proto
)

PEERDIR(
    bigrt/lib/consuming_system/config
    bigrt/lib/processing/shard_processor/stateless/config
    bigrt/lib/processing/shard_processor/fallback/config
    bigrt/lib/writer/swift/config
    bigrt/lib/writer/yt_queue/proto
    bigrt/lib/utility/liveness_checker/proto
    bigrt/lib/utility/throttler/proto
    ads/bsyeti/libs/profiling/solomon/proto
    ads/bsyeti/libs/tvm_manager/proto
    ads/bsyeti/libs/ytex/http/proto
    ads/bsyeti/libs/ytex/logging/proto
    grut/libs/client/factory/proto
    quality/user_sessions/rt/lib/common/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
