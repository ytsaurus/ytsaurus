RECURSE(
    yt
    chyt
    yql
    cpp
    go
    java
    python
    examples
    docs
    docker
    systest
)


IF (NOT OPENSOURCE)
    # Yandex-specific parts of YT.
    RECURSE(
        abcdapter
        admin
        benchmarks
        buildall
        cfg
        cron
        docs
        docs/yandex-specific
        docs/ytsaurus
        hermes
        idm-integration
        internal
        jaeger
        meta_scheduler
        microservices
        odin
        opensource
        packages
        recipe
        scripts
        skynet
        terraform
        yt_proto
    )
ENDIF()
