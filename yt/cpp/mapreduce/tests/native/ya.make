RECURSE(
    misc
    misc_core_http
    operations
    operations_core_http
)

IF (NOT OPENSOURCE AND NOT USE_VANILLA_PROTOC)
    RECURSE(
        check_initialized
        check_initialized/test_program
        crash_writer_on_exception
        crash_writer_on_exception/test_program
        error_exit
        error_exit/test_program
        parallel_cache_upload
        parallel_cache_upload/test_program
        remote_copy
        remote_copy/recipe
        job_on_exit_function
        portals
        proto_lib
        server_yt_name_conflicts
    )
ENDIF()
