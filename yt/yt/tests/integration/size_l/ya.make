PY3TEST()

SET(YT_SPLIT_FACTOR 55)

SET(YT_TIMEOUT 2400)

IF (SANITIZER_TYPE)
    SET(YT_TIMEOUT 2000)
ENDIF()

ENV(YT_TEST_FILTER=LARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

YT_SPEC(yt/yt/tests/integration/spec_job_batching.yson)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:48
        ram:56
    )
ELSE()
    REQUIREMENTS(
        cpu:16
        ram:20
    )
ENDIF()

END()
