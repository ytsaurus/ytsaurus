#include "stdafx.h"
#include "execution_stack.h"

#if defined(_unix_)
#   include <sys/mman.h>
#   include <limits.h>
#   include <unistd.h>
#   if !defined(__x86_64__)
#       error Unsupported platform
#   endif
#endif

#include <core/misc/error.h>
#include <core/misc/object_pool.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Stack sizes.
const size_t SmallExecutionStackSize = 1 << 18; // 256 Kb
const size_t LargeExecutionStackSize = 1 << 23; //   8 Mb

// Heap-backed stack.
template <size_t Size>
class THeapExecutionStack
    : public TExecutionStack
{
private:
    char* Base_;

public:
    THeapExecutionStack()
        : TExecutionStack(nullptr, RoundUpToPage(Size))
    {
        Base_ = new char[Size_ + 15];
        Stack_ = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(Base_ + 15) & ~static_cast<uintptr_t>(15));
    }

    ~THeapExecutionStack()
    {
        delete[] Base_;
    }
};

#ifdef _unix_
// Mapped memory with a few extra guard pages.
template <size_t Size, int GuardPages = 4>
class TProtectedExecutionStack
    : public TExecutionStack
{
private:
    char* Base_;

public:
    TProtectedExecutionStack()
        : TExecutionStack(nullptr, RoundUpToPage(Size))
    {
        const size_t guardSize = GuardPages * GetPageSize();

        Base_ = reinterpret_cast<char*>(::mmap(
                0,
                guardSize + Size_,
                PROT_READ | PROT_WRITE,
                MAP_ANONYMOUS | MAP_PRIVATE,
                -1,
                0));

        if (Base_ == MAP_FAILED) {
            THROW_ERROR_EXCEPTION("Failed to allocate execution stack")
                << TErrorAttribute("size", Size)
                << TErrorAttribute("guard_size", guardSize)
                << TError::FromSystem();
        }

        ::mprotect(Base_, guardSize, PROT_NONE);

        Stack_ = Base_ + guardSize;

        YCHECK((reinterpret_cast<uintptr_t>(Stack_) & 15) == 0);
    }

    ~TProtectedExecutionStack()
    {
        const size_t guardSize = GuardPages * GetPageSize();

        ::munmap(Base_, guardSize + Size_);
    }
};
#endif

////////////////////////////////////////////////////////////////////////////////

TExecutionStack::TExecutionStack(void* stack, size_t size)
    : Stack_(stack)
    , Size_(size)
{ }

TExecutionStack::~TExecutionStack()
{ }

void* TExecutionStack::GetStack()
{
    return Stack_;
}

const size_t TExecutionStack::GetSize()
{
    return Size_;
}

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStack stack)
{
#ifdef _unix_
    switch (stack) {
        case EExecutionStack::Small:
            return ObjectPool<TProtectedExecutionStack<SmallExecutionStackSize>>().Allocate();
        case EExecutionStack::Large:
            return ObjectPool<TProtectedExecutionStack<LargeExecutionStackSize>>().Allocate();
    }
#else
    switch (stack) {
        case EExecutionStack::Small:
            return ObjectPool<THeapExecutionStack<SmallExecutionStackSize>>().Allocate();
        case EExecutionStack::Large:
            return ObjectPool<THeapExecutionStack<LargeExecutionStackSize>>().Allocate();
    }
#endif
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, int GuardPages>
struct TPooledObjectTraits<NConcurrency::TProtectedExecutionStack<Size, GuardPages>, void>
    : public TPooledObjectTraitsBase
{
    typedef NConcurrency::TProtectedExecutionStack<Size, GuardPages> TStack;

    static void Clean(TStack* stack)
    {
#ifndef NDEBUG
        memset(stack->GetStack(), 0, stack->GetSize());
#endif
    }

    static int GetMaxPoolSize()
    {
        return 1024;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

