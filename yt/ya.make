RECURSE(
    admin
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
)


IF (NOT OPENSOURCE)
    # Yandex-specific parts of YT.
    RECURSE(
        abcdapter
        benchmarks
        buildall
        cfg
        cron
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
