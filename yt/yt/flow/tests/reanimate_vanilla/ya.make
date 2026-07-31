RECURSE(
    cpp
    cpp/pipeline
    python
    python/pipeline
)

IF (NOT OPENSOURCE)
    RECURSE(
        go
        go/pipeline
        java
        java/pipeline
    )
ENDIF()
