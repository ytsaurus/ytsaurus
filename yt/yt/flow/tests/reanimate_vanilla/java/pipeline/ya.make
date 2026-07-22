JAVA_PROGRAM(reanimate_bin)

JDK_VERSION(21)
WITH_JDK()

PEERDIR(
    yt/java/flow/flow-runner
    yt/java/flow/flow-spring-boot-starter

    contrib/java/javax/persistence/persistence-api/1.0
)

DEFAULT_JAVA_SRCS_LAYOUT()

LINT(base)

END()
