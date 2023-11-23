JTEST()

JDK_VERSION(11)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

IF(NOT EXPORT_GRADLE)
    JAVA_SRCS(
        PACKAGE_PREFIX tech.ytsaurus.typeinfo
        SRCDIR ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data *.txt
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/java/type-info
    contrib/java/junit/junit
)

DEPENDENCY_MANAGEMENT(
    contrib/java/junit/junit/4.13
)

END()
