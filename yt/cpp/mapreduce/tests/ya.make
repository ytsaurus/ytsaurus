RECURSE(
    native
)

IF (NOT OPENSOURCE)
    RECURSE(
        core_logging
        gtest_main
        lib
        util
        yt_initialize_hook
        yt_unittest_lib
    )

    IF (NOT USE_VANILLA_PROTOC)
        RECURSE(
            performance
        )
    ENDIF()

    IF (NOT USE_STL_SYSTEM)
        RECURSE(
            rpc_proxy
            rpc_proxy/run_test
        )
    ENDIF()
ENDIF()
