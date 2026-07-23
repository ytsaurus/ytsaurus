RECURSE(
    passthrough_transform
    types/common
    types/python
    types/python/pipeline
)

IF (NOT OPENSOURCE)
    RECURSE(
        all_states
        jvm_options
        retries
        types/java
        types/java/companion
    )
ENDIF()
