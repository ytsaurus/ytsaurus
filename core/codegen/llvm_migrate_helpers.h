#pragma once

#include <llvm/IR/Value.h>

#define LLVM_TEST(major, minor) (\
    defined(LLVM_VERSION_MAJOR) && LLVM_VERSION_MAJOR == (major) && \
    defined(LLVM_VERSION_MINOR) && LLVM_VERSION_MINOR == (minor))

namespace NYT {

template <class T>
llvm::Value *ConvertToPointer(T iterator)
{
#if LLVM_TEST(4, 0) || LLVM_TEST(5, 0)
    return &*iterator;
#else
    return iterator;
#endif
}
} // namespace NYT
