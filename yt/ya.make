RECURSE(
    admin
    benchmarks
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
    odin
    styleguide
    systest
    admin
    cron
)


IF (NOT OPENSOURCE)
    # Yandex-specific parts of YT.
    RECURSE(
        abcdapter
        buildall
        cfg
        docs
        docs/yandex-specific
        docs/ytsaurus
        idm-integration
        internal
        jaeger
        meta_scheduler
        microservices
        nirvana
        opensource
        packages
        python_py2
        recipe
        scripts
        skynet
        spark
        terraform
        yt_proto
        yt_sync
    )
ENDIF()
