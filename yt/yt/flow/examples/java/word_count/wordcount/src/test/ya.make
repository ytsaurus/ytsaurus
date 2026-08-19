JUNIT6()

JDK_VERSION(21)

SIZE(SMALL)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

PEERDIR(
    yt/java/flow/flow-test-utils
    yt/yt/flow/examples/java/word_count/wordcount
    contrib/java/org/junit/jupiter/junit-jupiter

    contrib/java/org/springframework/boot/spring-boot-test
    contrib/java/org/springframework/boot/spring-boot-test-autoconfigure

    contrib/java/org/assertj/assertj-core
    contrib/java/org/mockito/mockito-core
)

DEPENDENCY_MANAGEMENT(
    contrib/java/org/junit/jupiter/junit-jupiter/6.0.2
    contrib/java/org/junit/jupiter/junit-jupiter-api/6.0.2
    contrib/java/org/springframework/boot/spring-boot-test/4.0.2
    contrib/java/org/springframework/boot/spring-boot-test-autoconfigure/4.0.2
    contrib/java/org/assertj/assertj-core/3.27.6
    contrib/java/org/mockito/mockito-core/5.21.0
)

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

END()
