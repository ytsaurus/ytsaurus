GO_PROGRAM()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
)

IF (OS_WINDOWS)
    SRCS(
        mkwinsyscall.go
    )
ENDIF()

END()
