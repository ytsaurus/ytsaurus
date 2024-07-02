RECURSE(
    parallel_io
    user_job_statistics
)

IF (NOT OPENSOURCE)
    RECURSE(
        blob
        blob/protos
        blob/tools
        blob/tools/file-yt
        blob/tools/file-yt/tests
        blob_table
        cypress_path
        lazy_sort
        lambda
        llvm_profile
        path_template
        skynet_table
        table_schema
    )
ENDIF()
