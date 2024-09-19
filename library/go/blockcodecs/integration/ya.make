GO_TEST()

DEPENDS(library/go/blockcodecs/integration/cpp)

IF (RACE)
    ENV(BLOCKCODECS_FLAT_TEST=1)
ENDIF()

SIZE(MEDIUM)

REQUIREMENTS(ram:32)

EXPLICIT_DATA()

GO_TEST_SRCS(
    decoder_test.go
)

IF (NOT OPENSOURCE)
    GO_TEST_SRCS(
        compat_test.go
    )
ENDIF()

END()

RECURSE(cpp)
