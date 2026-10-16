RECURSE(
    all_states_cpp
    http_client
    passthrough_transform
    resource/common
    resource/cpp
    resource/cpp/companion
    resource/java
    resource/java/companion
    resource/python
    resource/python/pipeline
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
        types/go
        types/go/pipeline
    )
ENDIF()
