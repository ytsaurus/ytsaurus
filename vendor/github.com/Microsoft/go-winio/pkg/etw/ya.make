GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
    eventdescriptor.go
    level_string.go
    opcode_string.go
)

IF (OS_WINDOWS)
    SRCS(
        eventdata.go
        eventdatadescriptor.go
        eventmetadata.go
        eventopt.go
        fieldopt.go
        newprovider.go
        provider.go
        providerglobal.go
        ptr64_64.go
        syscall.go
        wrapper_64.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(provider_test.go)
ENDIF()

END()

RECURSE(
    gotest
)

IF (OS_LINUX)
    RECURSE(
        sample
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        sample
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        sample
    )
ENDIF()
