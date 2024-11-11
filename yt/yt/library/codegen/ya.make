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
    contrib/libs/llvm18/lib/IR
    contrib/libs/llvm18/lib/AsmParser
    contrib/libs/llvm18/lib/CodeGen
    contrib/libs/llvm18/lib/ExecutionEngine
    contrib/libs/llvm18/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm18/lib/IRReader
    contrib/libs/llvm18/lib/Linker
    contrib/libs/llvm18/lib/Transforms/IPO
    contrib/libs/llvm18/lib/Support
    contrib/libs/llvm18/lib/Target
    contrib/libs/llvm18/lib/Target/X86
    contrib/libs/llvm18/lib/Target/X86/AsmParser
    contrib/libs/llvm18/lib/Target/WebAssembly
    contrib/libs/llvm18/lib/Target/WebAssembly/AsmParser
    contrib/libs/llvm18/lib/Passes
    yt/yt/core
)

END()
