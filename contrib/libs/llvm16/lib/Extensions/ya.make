# Generated by devtools/yamaker.

LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(16.0.0)

LICENSE(NCSA)

PEERDIR(
    contrib/libs/llvm16
    contrib/libs/llvm16/lib/Support
    contrib/libs/llvm16/tools/polly/lib
    contrib/libs/llvm16/tools/polly/lib/External/isl
    contrib/libs/llvm16/tools/polly/lib/External/ppcg
)

ADDINCL(
    contrib/libs/llvm16/lib/Extensions
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    Extensions.cpp
)

END()
