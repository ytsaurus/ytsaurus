LIBRARY()

SRCS(
    spilling_imp.cpp
    spilling_imp.h
    namespaces_list.h
    namespaces_list.cpp
    namespace_cache.h
    namespace_cache.cpp
    interface/spilling.h
    storage/file_storage/file_storage.cpp
    storage/storage.h
    storage/storage.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/binary_json
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core/spilling/storage
)

NO_COMPILER_WARNINGS()

IF (NOT MKQL_DISABLE_CODEGEN)
    PEERDIR(
        contrib/ydb/library/yql/minikql/codegen
        contrib/libs/llvm12/lib/IR
        contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
        contrib/libs/llvm12/lib/Linker
        contrib/libs/llvm12/lib/Target/X86
        contrib/libs/llvm12/lib/Target/X86/AsmParser
        contrib/libs/llvm12/lib/Transforms/IPO
    )
ELSE()
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    storage
)

RECURSE_FOR_TESTS(
    ut
)
