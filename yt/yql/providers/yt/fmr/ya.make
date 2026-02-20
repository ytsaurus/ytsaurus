RECURSE(
    coordinator
    file
    fmr_tool_lib
    gc_service
    job
    job_factory
    job_launcher
    job_preparer
    process
    proto
    request_options
    table_data_service
    tests
    test_tools
    tvm
    utils
    worker
    yt_job_service
)

IF (NOT OPENSOURCE)
    RECURSE(recipe)
ENDIF()

RECURSE_FOR_TESTS(
    tests
    ut
)
