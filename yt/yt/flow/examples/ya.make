RECURSE(
    cpp
    docker
    go
    java
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        kotlin
    )
ENDIF()
