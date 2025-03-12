RECURSE(
    dynamic_tables
    generate_documentation_release_notes
    generate_github_release_notes
)

IF (NOT OPENSOURCE)
    RECURSE(
        analyze_solomon_shard
        build_snapshots
        ci
        create_admin_commands_aco
        delete_solomon_metrics
        find_top_operations
        get_versions
        job_perforator
        lvc-hunter
        mispick
        used_disk_space
        scrape_cofe_metrics
        tutorial
        upload_resource
        copy_pool_structure
        dump_operation_input_tables
        erase_corrupted_chunk
        query_tracker
        ref_counted_diff
        scheduler
        master
        python_sdk
        cmake
        cache_heater
    )

    RECURSE_FOR_TESTS(
        master
    )
ENDIF()
