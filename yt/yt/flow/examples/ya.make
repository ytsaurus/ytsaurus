RECURSE(
    cpp
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        docker
        java
        kotlin
    )
ENDIF()
