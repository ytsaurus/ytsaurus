RECURSE(
    cpp
    go
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        docker
        java
        kotlin
    )
ENDIF()
