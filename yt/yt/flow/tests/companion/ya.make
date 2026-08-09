RECURSE(
    all_states_cpp
    passthrough_transform
    resource/common
    resource/cpp
    resource/cpp/companion
    types/common
    types/python
    types/python/pipeline
)

IF (NOT OPENSOURCE)
    RECURSE(
        all_states
        jvm_options
        retries
        types/go
        types/go/pipeline
        types/java
        types/java/companion
    )
ENDIF()
