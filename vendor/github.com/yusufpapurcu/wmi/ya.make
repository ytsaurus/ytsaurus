GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.2.4)

IF (OS_WINDOWS)
    SRCS(
        swbemservices.go
        wmi.go
    )

    GO_TEST_SRCS(
        swbemservices_test.go
        wmi_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
