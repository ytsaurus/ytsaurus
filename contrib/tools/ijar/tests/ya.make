PY3TEST()

WITHOUT_LICENSE_TEXTS()

TEST_SRCS(
    test_mapstruct_abi.py
)

SRCDIR(build/scripts)

PY_SRCS(
    NAMESPACE build.scripts
    canonicalize_java_abi_jar.py
)

DATA(
    arcadia/build/conf/copy_kotlin_inline_sam_classes.py
    arcadia/build/scripts/fs_tools.py
    arcadia/build/scripts/process_command_files.py
    arcadia/devtools/dummy_arcadia/kotlin/abi_jar/producer/Api.kt
    arcadia/devtools/dummy_arcadia/kotlin/abi_jar/producer/JavaApi.java
    arcadia/devtools/dummy_arcadia/kotlin/abi_jar/middle/Middle.kt
    arcadia/devtools/dummy_arcadia/kotlin/abi_jar/consumer/Main.kt
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
