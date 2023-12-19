JAVA_LIBRARY()

JDK_VERSION(11)

CHECK_JAVA_DEPS(yes)

PEERDIR(
    contrib/java/com/google/code/findbugs/jsr305
    contrib/java/org/hamcrest/hamcrest-core

    yt/java/annotations
    yt/java/ytsaurus-testlib/src/main/proto
)

JAVA_SRCS(SRCDIR src/main/java **/*)

# Added automatically to remove dependency on default contrib versions
DEPENDENCY_MANAGEMENT(
    contrib/java/com/google/code/findbugs/jsr305/3.0.2
    contrib/java/org/hamcrest/hamcrest-core/2.2
)

LINT(base)
END()
