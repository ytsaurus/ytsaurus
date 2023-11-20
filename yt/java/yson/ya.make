JAVA_LIBRARY(ytsaurus-yson)

IF(JDK_VERSION == "")
    JDK_VERSION(11)
ENDIF()

LINT(extended)

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

CHECK_JAVA_DEPS(yes)

PEERDIR(
    contrib/java/com/google/code/findbugs/jsr305

    yt/java/annotations
)

DEFAULT_JAVA_SRCS_LAYOUT()

# Added automatically to remove dependency on default contrib versions
DEPENDENCY_MANAGEMENT(
    contrib/java/com/google/code/findbugs/jsr305/3.0.2
)

END()

RECURSE_FOR_TESTS(
    src/test
)
