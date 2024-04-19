RECURSE(
    native
)

IF (NOT OPENSOURCE)
    RECURSE(
        gtest_main
        lib
        performance
        rpc_proxy
        rpc_proxy/run_test
        util
        yt_initialize_hook
        yt_unittest_lib
    )
ENDIF()
