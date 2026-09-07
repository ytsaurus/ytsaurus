PY3TEST()

WITHOUT_LICENSE_TEXTS()

TEST_SRCS(
    test_mapstruct_abi.py
)

PEERDIR(
    build/platform/java/jdk
    build/platform/java/kotlin
    ${JDK_RESOURCE_PEERDIR}
)

DEPENDS(
    contrib/java/org/mapstruct/mapstruct/1.5.5.Final
    contrib/java/org/mapstruct/mapstruct-processor/1.5.5.Final
    contrib/java/org/jetbrains/kotlin/kotlin-stdlib/2.3.10
    contrib/tools/ijar/tests/source
)

END()
