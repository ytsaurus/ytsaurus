RECURSE(
    passthrough_transform
    types/common
    types/java
    types/java/companion
    types/python
    types/python/pipeline
)

IF (NOT OPENSOURCE)
    RECURSE(
        all_states
        jvm_options
        retries
    )
ENDIF()
