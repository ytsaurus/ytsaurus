RECURSE(
    cpp
    java
    kotlin
    python
)

IF (NOT OPENSOURCE)
    RECURSE(
        docker
    )
ENDIF()
