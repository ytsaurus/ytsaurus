PY3_LIBRARY()

PY_SRCS(
    __init__.py
    conftest.py
    default_config_parameters.py
    flow_process.py
    helpers.py
    yt_flow_base.py
    yt_flow_java_base.py
    yt_flow_python_base.py
)

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    PEERDIR(
        contrib/python/mergedeep
        contrib/python/requests
        library/python/port_manager
        library/python/testing/yatest_common
        yt/yt/flow/library/python/bullied_process
        yt/recipe/basic/lib
        yt/python/yt/wrapper
        yt/python/yt/environment
    )
ENDIF()

END()
