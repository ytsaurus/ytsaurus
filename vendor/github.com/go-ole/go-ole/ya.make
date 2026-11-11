GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.3.0)

SRCS(
    connect.go
    constants.go
    error.go
    guid.go
    iconnectionpoint.go
    iconnectionpointcontainer.go
    idispatch.go
    ienumvariant.go
    iinspectable.go
    iprovideclassinfo.go
    itypeinfo.go
    iunknown.go
    ole.go
    safearray.go
    safearrayconversion.go
    utility.go
    variant.go
    vt_string.go
)

GO_TEST_SRCS(
    guid_test.go
    safearray_test.go
)

IF (ARCH_X86_64)
    SRCS(
        variant_amd64.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        variant_arm64.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        com_func.go
        error_func.go
        iconnectionpoint_func.go
        iconnectionpointcontainer_func.go
        idispatch_func.go
        ienumvariant_func.go
        iinspectable_func.go
        iprovideclassinfo_func.go
        itypeinfo_func.go
        iunknown_func.go
        safearray_func.go
        winrt_doc.go
    )

    GO_TEST_SRCS(
        com_func_test.go
        connect_test.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM6 OR OS_LINUX AND ARCH_ARM7)
    SRCS(
        variant_arm.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        com_func.go
        error_func.go
        iconnectionpoint_func.go
        iconnectionpointcontainer_func.go
        idispatch_func.go
        ienumvariant_func.go
        iinspectable_func.go
        iprovideclassinfo_func.go
        itypeinfo_func.go
        iunknown_func.go
        safearray_func.go
        winrt_doc.go
    )

    GO_TEST_SRCS(
        com_func_test.go
        connect_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        com.go
        error_windows.go
        iconnectionpoint_windows.go
        iconnectionpointcontainer_windows.go
        idispatch_windows.go
        ienumvariant_windows.go
        iinspectable_windows.go
        iprovideclassinfo_windows.go
        itypeinfo_windows.go
        iunknown_windows.go
        safearray_windows.go
        safearrayslices.go
        variables.go
        winrt.go
    )

    GO_TEST_SRCS(
        com_test.go
        connect_windows_test.go
        idispatch_windows_test.go
        ienumvariant_test.go
        iunknown_windows_test.go
        safearrayconversion_test.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        variant_date_amd64.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        variant_date_arm64.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        com_func.go
        error_func.go
        iconnectionpoint_func.go
        iconnectionpointcontainer_func.go
        idispatch_func.go
        ienumvariant_func.go
        iinspectable_func.go
        iprovideclassinfo_func.go
        itypeinfo_func.go
        iunknown_func.go
        safearray_func.go
        winrt_doc.go
    )

    GO_TEST_SRCS(
        com_func_test.go
        connect_test.go
    )
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        com_func.go
        error_func.go
        iconnectionpoint_func.go
        iconnectionpointcontainer_func.go
        idispatch_func.go
        ienumvariant_func.go
        iinspectable_func.go
        iprovideclassinfo_func.go
        itypeinfo_func.go
        iunknown_func.go
        safearray_func.go
        winrt_doc.go
    )

    GO_TEST_SRCS(
        com_func_test.go
        connect_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
    oleutil
)
