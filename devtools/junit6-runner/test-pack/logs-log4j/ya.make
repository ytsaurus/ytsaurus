NEED_CHECK()

JUNIT6()

ANNOTATION_PROCESSOR(
    lombok.launch.AnnotationProcessorHider${"$"}AnnotationProcessor
)

LINT(extended)

JAVA_SRCS(SRCDIR java **/*)
JAVA_SRCS(SRCDIR resources **/*)

INCLUDE(${ARCADIA_ROOT}/devtools/junit6-runner/test-pack/test/ya.make.test.dependency_management.inc)

PEERDIR(
    devtools/junit6-runner

    contrib/java/log4j/log4j

    contrib/java/org/projectlombok/lombok
)

END()
