# Generated by devtools/yamaker.

LIBRARY()

VERSION(18.1.8)

LICENSE(Apache-2.0 WITH LLVM-exception)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/llvm18
    contrib/libs/llvm18/include
    contrib/libs/llvm18/lib/CodeGen/LowLevelType
    contrib/libs/llvm18/lib/MC
    contrib/libs/llvm18/lib/MC/MCParser
    contrib/libs/llvm18/lib/MCA
    contrib/libs/llvm18/lib/Support
    contrib/libs/llvm18/lib/Target/X86/MCTargetDesc
    contrib/libs/llvm18/lib/Target/X86/TargetInfo
    contrib/libs/llvm18/lib/TargetParser
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/llvm18/lib/Target/X86
    contrib/libs/llvm18/lib/Target/X86
    contrib/libs/llvm18/lib/Target/X86/MCA
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    X86CustomBehaviour.cpp
)

END()
