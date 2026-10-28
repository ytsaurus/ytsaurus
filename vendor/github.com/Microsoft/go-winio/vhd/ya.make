GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_WINDOWS)
    SRCS(
        vhd.go
        zvhd_windows.go
    )
ENDIF()

END()
