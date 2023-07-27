OWNER(g:yt)

GO_TEST()

GO_TEST_SRCS(
    py_test.go
)

SRCS(
    shutil.go
)

IF (USE_SYSTEM_PYTHON)
    PEERDIR(
        build/platform/python
    )
ELSE()
    DEPENDS(
        contrib/tools/python
    )
ENDIF()

DEPENDS(
    yt/yt/python/yson_shared
    yt/yt/python/driver/rpc_shared

    yt/yt/server/all
    yt/yt/tools/yt_sudo_fixup
)

DATA(
    arcadia/yt/python
    arcadia/yt/yt/python/yt_yson_bindings
    arcadia/yt/yt/python/yt_driver_bindings
    arcadia/yt/yt/python/yt_driver_rpc_bindings
    arcadia/certs

    arcadia/yt/python/yt/wrapper/tests/system_python/contrib

    arcadia/contrib/python/simplejson
    arcadia/contrib/python/cloudpickle
    arcadia/contrib/python/charset-normalizer
    arcadia/contrib/python/decorator
    arcadia/contrib/deprecated/python/backports-abc
    arcadia/contrib/deprecated/python/singledispatch
    arcadia/contrib/python/tornado/tornado-4
    arcadia/contrib/python/typing-extensions
    arcadia/contrib/deprecated/python/typing
    arcadia/contrib/python/tqdm
    arcadia/contrib/python/chardet
    arcadia/contrib/python/idna
    arcadia/contrib/python/six

    arcadia/yt/python/yt/type_info

    sbr://2340668090 # Dynamic libraries
)

SIZE(LARGE)

TAG(
    ya:yt
    ya:fat
    ya:force_sandbox
    ya:huge_logs
    ya:full_logs
    ya:sys_info
    ya:noretries
)

REQUIREMENTS(
    ram:16
    cpu:4
    ram_disk:4
    sb_vault:YT_TOKEN=value:ignat:robot-yt-test-token
)

YT_SPEC(yt/python/yt/wrapper/tests/system_python/spec.yson)

END()
