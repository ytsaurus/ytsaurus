RECURSE(
    cpp
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        docker
        go
        java
        kotlin
    )
ENDIF()
