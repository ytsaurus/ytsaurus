RECURSE(
    cpp
    cpp/pipeline
    python
    python/pipeline
)

IF (NOT OPENSOURCE)
    RECURSE(
        java
        java/pipeline
    )
ENDIF()
