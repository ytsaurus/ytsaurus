GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.4.4)

IF (OS_LINUX)
    SRCS(
        nodata_linux.go
        xattr.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        nodata_unix.go
        xattr.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        xattr_unsupported.go
    )
ENDIF()

END()
