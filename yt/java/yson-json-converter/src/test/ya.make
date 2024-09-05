JTEST()

JDK_VERSION(11)

SIZE(SMALL)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/java/yson-json-converter
    contrib/java/junit/junit
)

DEPENDENCY_MANAGEMENT(
    contrib/java/junit/junit/4.13
)

TEST_CWD(yt/java/yson-json-converter)

LINT(extended)

END()
