IF (HOST_OS_DARWIN AND HOST_ARCH_X86_64)
    DECLARE_EXTERNAL_RESOURCE(TEST_TOOL_HOST sbr:8029664104)
ELSEIF (HOST_OS_DARWIN AND HOST_ARCH_ARM64)
    DECLARE_EXTERNAL_RESOURCE(TEST_TOOL_HOST sbr:8029660543)
ELSEIF (HOST_OS_LINUX AND HOST_ARCH_X86_64)
    DECLARE_EXTERNAL_RESOURCE(TEST_TOOL_HOST sbr:8029671029)
ELSEIF (HOST_OS_LINUX AND HOST_ARCH_AARCH64)
    DECLARE_EXTERNAL_RESOURCE(TEST_TOOL_HOST sbr:8029657728)
ELSEIF (HOST_OS_WINDOWS AND HOST_ARCH_X86_64)
    DECLARE_EXTERNAL_RESOURCE(TEST_TOOL_HOST sbr:8029667517)

ENDIF()
