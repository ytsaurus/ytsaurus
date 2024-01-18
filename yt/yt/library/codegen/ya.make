LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    builder_base.cpp
    init.cpp
    module.cpp
    routine_registry.cpp
    msan.cpp
)

PEERDIR(
    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/AsmParser
    contrib/libs/llvm16/lib/CodeGen
    contrib/libs/llvm16/lib/ExecutionEngine
    contrib/libs/llvm16/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm16/lib/IRReader
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Support
    contrib/libs/llvm16/lib/Target
    contrib/libs/llvm16/lib/Target/X86
    contrib/libs/llvm16/lib/Target/X86/AsmParser
    contrib/libs/llvm16/lib/Target/WebAssembly
    contrib/libs/llvm16/lib/Target/WebAssembly/AsmParser
    contrib/libs/llvm16/lib/Passes
    yt/yt/core
)

END()
