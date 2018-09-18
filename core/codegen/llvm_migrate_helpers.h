#pragma once

#include <llvm/IR/Value.h>
#include <llvm/Config/llvm-config.h>

#define LLVM_VERSION_GE(major, minor) (\
    defined(LLVM_VERSION_MAJOR) && defined(LLVM_VERSION_MINOR) && \
    LLVM_VERSION_MAJOR > (major) || LLVM_VERSION_MAJOR == (major) && \
    LLVM_VERSION_MINOR >= (minor))

namespace NYT {

template <class T>
llvm::Value *ConvertToPointer(T iterator)
{
#if LLVM_VERSION_GE(3, 9)
    return &*iterator;
#else
    return iterator;
#endif
}
} // namespace NYT
