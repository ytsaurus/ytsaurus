RECURSE(
    cpp
    cpp/pipeline
    java
    java/pipeline
    python
    python/pipeline
)

IF (NOT OPENSOURCE)
    RECURSE(
        go
        go/pipeline
    )
ENDIF()
