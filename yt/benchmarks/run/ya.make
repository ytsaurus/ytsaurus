PY3_PROGRAM(
    yt-run-benchmark
)

PY_SRCS(
    __init__.py
    __main__.py
    common.py
    qt_run.py
    upload_metrics.py
)

PEERDIR(
    contrib/python/click
    contrib/python/httpx
    yt/python/client
    yt/python/yt
    yt/python/yt/wrapper
)

ALL_RESOURCE_FILES(
    PREFIX yt/benchmarks/run/
    sql
    queries queries_optimized
)

ALL_RESOURCE_FILES(
    PREFIX yt/benchmarks/run/pragmas STRIP public_pragmas
    sql
    public_pragmas
)

IF (NOT OPENSOURCE) 
    PY_SRCS(
        yql_run.py
    )

    PEERDIR(
        yql/library/python
    )
    
    ALL_RESOURCE_FILES(
        PREFIX yt/benchmarks/run/pragmas STRIP private_pragmas
        sql
        private_pragmas
    )
ENDIF()

END()
