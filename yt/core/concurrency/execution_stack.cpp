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

////////////////////////////////////////////////////////////////////////////////

TExecutionStack::TExecutionStack(size_t size)
    : Stack_(nullptr)
    , Size_(size)
{ }

TExecutionStack::~TExecutionStack()
{ }

void* TExecutionStack::GetStack() const
{
    return Stack_;
}

size_t TExecutionStack::GetSize() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

#if defined(_unix_)

//! Mapped memory with a few extra guard pages.
template <size_t Size>
class TExecutionStackImpl
    : public TExecutionStack
{
public:
    TExecutionStackImpl()
        : TExecutionStack(RoundUpToPage(Size))
    {
        const size_t guardSize = GuardPages * GetPageSize();

        int flags =
#if defined(_darwin_)
            MAP_ANON | MAP_PRIVATE;
#else
            MAP_ANONYMOUS | MAP_PRIVATE;
#endif

        Base_ = reinterpret_cast<char*>(::mmap(
            0,
            guardSize + Size_,
            PROT_READ | PROT_WRITE,
            flags,
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

    ~TExecutionStackImpl()
    {
        const size_t guardSize = GuardPages * GetPageSize();
        ::munmap(Base_, guardSize + Size_);
    }

private:
    char* Base_;

    static const int GuardPages = 4;

};

#elif defined(_win_)

template <size_t Size>
class TExecutionStackImpl
    : public TExecutionStack
{
public:
    TExecutionStackImpl()
        : TExecutionStack(RoundUpToPage(Size))
    { }
};

#else
#   error Unsupported platform
#endif

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStack stack)
{
    switch (stack) {
        case EExecutionStack::Small:
            return ObjectPool<TExecutionStackImpl<SmallExecutionStackSize>>().Allocate();
        case EExecutionStack::Large:
            return ObjectPool<TExecutionStackImpl<LargeExecutionStackSize>>().Allocate();
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size>
struct TPooledObjectTraits<NConcurrency::TExecutionStackImpl<Size>, void>
    : public TPooledObjectTraitsBase
{
    typedef NConcurrency::TExecutionStackImpl<Size> TStack;

    static void Clean(TStack* stack)
    {
#ifndef NDEBUG
        if (stack->GetStack()) {
            memset(stack->GetStack(), 0, stack->GetSize());
        }
#endif
    }

    static int GetMaxPoolSize()
    {
        return 1024;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

