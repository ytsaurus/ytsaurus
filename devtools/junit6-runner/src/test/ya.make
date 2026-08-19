NEED_CHECK()

JUNIT6()

LINT(extended)

JAVA_SRCS(SRCDIR java **/*)
JAVA_SRCS(RESOURCES SRCDIR resources **/*)

ANNOTATION_PROCESSOR(
    lombok.launch.AnnotationProcessorHider${"$"}AnnotationProcessor
)

PEERDIR(
    devtools/jtest/src/test
    devtools/junit6-runner
    devtools/junit6-runner/test-pack/test
)

END()
