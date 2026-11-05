PY3TEST()

SET(YT_SPLIT_FACTOR 60)

ENV(YT_TEST_FILTER=SMALL)

INCLUDE(../YaMakeDependsBoilerplate.txt)
TIMEOUT(2400)

YT_SPEC(yt/yt/tests/integration/spec_job_batching.yson)

IF (SANITIZER_TYPE == "thread")
    REQUIREMENTS(
        ram:34
        cpu:50
    )
ELSEIF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:34
        cpu:46
    )
ELSE()
    REQUIREMENTS(
        ram:14
        cpu:12
    )
ENDIF()

END()
