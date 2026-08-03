RECURSE(
    cpp
    docker
    go
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        java
        kotlin
    )
ENDIF()
