IF (NOT OPENSOURCE)
    RECURSE(
        components
        job_tools
        unn
        unn-docs
        retry_wrapper
        tools
    )
ENDIF()
