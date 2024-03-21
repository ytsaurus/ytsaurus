PY3_LIBRARY()

VERSION(5.9.8)

LICENSE(BSD-3-Clause)

NO_COMPILER_WARNINGS()

NO_LINT()

NO_CHECK_IMPORTS(
    psutil._psaix
    psutil._psbsd
    psutil._pslinux
    psutil._psosx
    psutil._pssunos
    psutil._psutil_bsd
    psutil._psutil_common
    psutil._psutil_osx
    psutil._psutil_sunos
    psutil._psutil_windows
    psutil._pswindows
)

NO_UTIL()

CFLAGS(
    -DPSUTIL_VERSION=598
)

SRCS(
    psutil/_psutil_common.c
)

IF (OS_LINUX)
    CFLAGS(
        -DPSUTIL_POSIX=1
        -DPSUTIL_LINUX=1
    )

    SRCS(
        psutil/_psutil_linux.c
        psutil/_psutil_posix.c
        psutil/arch/linux/disk.c
        psutil/arch/linux/mem.c
        psutil/arch/linux/net.c
        psutil/arch/linux/proc.c
        psutil/arch/linux/users.c
    )

    PY_REGISTER(
        psutil._psutil_linux
        psutil._psutil_posix
    )
ENDIF()

IF (OS_DARWIN)
    CFLAGS(
        -DPSUTIL_POSIX=1
        -DPSUTIL_OSX=1
    )

    LDFLAGS(
        -framework CoreFoundation
        -framework IOKit
    )

    SRCS(
        psutil/_psutil_osx.c
        psutil/_psutil_posix.c
        psutil/arch/osx/cpu.c
        psutil/arch/osx/disk.c
        psutil/arch/osx/mem.c
        psutil/arch/osx/net.c
        psutil/arch/osx/proc.c
        psutil/arch/osx/sensors.c
        psutil/arch/osx/sys.c
    )

    PY_REGISTER(
        psutil._psutil_osx
        psutil._psutil_posix
    )
ENDIF()

IF (OS_WINDOWS)
    CFLAGS(
        -DPSUTIL_WINDOWS=1
        -DPSUTIL_SIZEOF_PID_T=4
    )

    LDFLAGS(
        Shell32.lib
        PowrProf.lib
        Wtsapi32.lib
        Pdh.lib
    )

    SRCS(
        psutil/_psutil_windows.c
        psutil/arch/windows/cpu.c
        psutil/arch/windows/disk.c
        psutil/arch/windows/mem.c
        psutil/arch/windows/net.c
        psutil/arch/windows/proc.c
        psutil/arch/windows/proc_handles.c
        psutil/arch/windows/proc_info.c
        psutil/arch/windows/proc_utils.c
        psutil/arch/windows/security.c
        psutil/arch/windows/sensors.c
        psutil/arch/windows/services.c
        psutil/arch/windows/socks.c
        psutil/arch/windows/sys.c
        psutil/arch/windows/wmi.c
    )

    PY_REGISTER(
        psutil._psutil_windows
    )
ENDIF()

PY_SRCS(
    TOP_LEVEL
    psutil/__init__.py
    psutil/_common.py
    psutil/_compat.py
    psutil/_psaix.py
    psutil/_psbsd.py
    psutil/_pslinux.py
    psutil/_psosx.py
    psutil/_psposix.py
    psutil/_pssunos.py
    psutil/_pswindows.py
)

RESOURCE_FILES(
    PREFIX contrib/python/psutil/py3/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()