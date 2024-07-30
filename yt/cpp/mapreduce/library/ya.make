RECURSE(
    parallel_io
    user_job_statistics
)

IF (NOT OPENSOURCE)
    IF (NOT USE_VANILLA_PROTOC)
        RECURSE(
            blob/tools
            blob/tools/file-yt
            blob/tools/file-yt/tests
            blob
            blob/protos
            skynet_table
            lambda
        )
    ENDIF()

    RECURSE(
        blob_table
        get_gpu_cluster
        cypress_path
        lazy_sort
        llvm_profile
        path_template
        table_schema
    )
ENDIF()
