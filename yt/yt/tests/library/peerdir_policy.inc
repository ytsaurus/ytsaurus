PEERDIR_FROM_POLICY(
    ALLOW
    yt/.*
    # TODO: Remove after YT-20279.
    ads/caesar/libs/yt_sync/test_chaos
    bigrt/demo/caesar/test/exactly_once
)

PEERDIR_FROM_POLICY(
    DENY
    .*
)
