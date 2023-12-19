PY3TEST()

TEST_SRCS(
    test_local_mode.py
)

PEERDIR(
    yt/python/yt/yson
    yt/python/yt/local
    yt/python/yt/environment
    yt/python/yt/environment/arcadia_interop
    yt/yt/python/yt_yson_bindings
)

DEPENDS(
    yt/python/yt/local/bin/yt_local_make
    yt/yt/packages/tests_package
)

SIZE(MEDIUM)

FORK_SUBTESTS()

SPLIT_FACTOR(6)

IF (YT_TEAMCITY)
    TAG(ya:yt)

    YT_SPEC(yt/yt/tests/integration/spec.yson)

    REQUIREMENTS(
        ram_disk:32
        ram:48
        cpu:4
    )
ELSE()
    REQUIREMENTS(
        ram_disk:4
        ram:16
        cpu:4
    )
ENDIF()

# TODO(babenko): DEVTOOLSSUPPORT-32901
IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

TAG(
    ya:force_distbuild
    ya:huge_logs
    ya:full_logs
)

END()
