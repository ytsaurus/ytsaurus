GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		common.go
		format.go
		reader.go
		stat_actime2.go
		stat_unix.go
		strconv.go
		writer.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		common.go
		format.go
		reader.go
		stat_actime2.go
		stat_unix.go
		strconv.go
		writer.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		common.go
		format.go
		reader.go
		stat_actime1.go
		stat_unix.go
		strconv.go
		writer.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		common.go
		format.go
		reader.go
		stat_actime1.go
		stat_unix.go
		strconv.go
		writer.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		common.go
		format.go
		reader.go
		strconv.go
		writer.go
    )
ENDIF()
END()
