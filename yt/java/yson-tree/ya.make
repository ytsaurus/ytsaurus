JAVA_LIBRARY()

DEFAULT_JDK_VERSION(11)

LINT(extended)

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

CHECK_JAVA_DEPS(yes)

PEERDIR(
    yt/java/annotations
    yt/java/yson
)

DEFAULT_JAVA_SRCS_LAYOUT()

END()

RECURSE_FOR_TESTS(
    src/test
)
