NEED_CHECK()

JUNIT6()

ANNOTATION_PROCESSOR(
    lombok.launch.AnnotationProcessorHider${"$"}AnnotationProcessor
)

IF(YA_IDE_GRADLE)
    JVM_ARGS("-DIDE_TEST=true")
ENDIF()

LINT(extended)

JAVA_SRCS(SRCDIR java **/*)
JAVA_SRCS(SRCDIR resources **/*)

INCLUDE(${ARCADIA_ROOT}/devtools/junit6-runner/test-pack/test/ya.make.test.dependency_management.inc)

PEERDIR(
    devtools/junit6-runner

    contrib/java/org/slf4j/slf4j-api
    contrib/java/org/slf4j/slf4j-simple

    contrib/java/org/projectlombok/lombok
)

END()
