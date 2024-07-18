PY3TEST()

ENV(YT_TEST_FILTER=MEDIUM)

# Set YT variables before INCLUDE
IF (SANITIZER_TYPE)
    SET(YT_SPLIT_FACTOR 70)
    SET(YT_TIMEOUT 2200)
ELSE()
    SET(YT_SPLIT_FACTOR 70)
    SET(YT_TIMEOUT 2000)
ENDIF()

INCLUDE(../YaMakeDependsBoilerplate.txt)

# Set REQUIREMENTS after INCLUDE for proper override
IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:42
        ram:44
    )
ELSE()
    REQUIREMENTS(
        cpu:22
        ram:18
    )
ENDIF()

END()
