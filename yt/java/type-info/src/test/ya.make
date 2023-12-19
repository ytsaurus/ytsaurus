JTEST()

JDK_VERSION(11)

JAVA_SRCS(SRCDIR java **/*)

JAVA_SRCS(SRCDIR resources **/*)

JAVA_SRCS(
    PACKAGE_PREFIX tech.ytsaurus.typeinfo
    SRCDIR ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data *.txt
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/java/type-info
    contrib/java/junit/junit
)

DEPENDENCY_MANAGEMENT(
    contrib/java/junit/junit/4.13
)

END()
