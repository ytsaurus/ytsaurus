JAVA_LIBRARY(ytsaurus-annotations)

IF(JDK_VERSION == "")
    JDK_VERSION(11)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

ENABLE(SOURCES_JAR)

USE_ERROR_PRONE()
LINT(extended)
CHECK_JAVA_DEPS(yes)

DEFAULT_JAVA_SRCS_LAYOUT()

PEERDIR(
    contrib/java/com/google/code/findbugs/jsr305/3.0.2
)

END()
