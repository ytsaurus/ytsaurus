GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
    ea.go
)

IF (OS_WINDOWS)
    SRCS(
        backup.go
        file.go
        fileinfo.go
        hvsock.go
        pipe.go
        privilege.go
        reparse.go
        sd.go
        syscall.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(
        backup_test.go
        ea_test.go
        fileinfo_test.go
        hvsock_go118_test.go
        hvsock_test.go
        pipe_test.go
        privileges_test.go
        sd_test.go
    )
ENDIF()

END()

RECURSE(
    backuptar
    gotest
    internal
    pkg
    tools
)

IF (OS_WINDOWS)
    RECURSE(
        wim
        vhd
    )
ENDIF()
