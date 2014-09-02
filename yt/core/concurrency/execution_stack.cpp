#include "stdafx.h"
#include "execution_stack.h"
#include "execution_context.h"

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

TExecutionStackBase::TExecutionStackBase(size_t size)
    : Stack_(nullptr)
    , Size_(RoundUpToPage(size))
{ }

TExecutionStackBase::~TExecutionStackBase()
{ }

void* TExecutionStackBase::GetStack() const
{
    return Stack_;
}

size_t TExecutionStackBase::GetSize() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

#if defined(_unix_)

TExecutionStack::TExecutionStack(size_t size)
    : TExecutionStackBase(size)
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
            << TErrorAttribute("size", Size_)
            << TErrorAttribute("guard_size", guardSize)
            << TError::FromSystem();
    }

    ::mprotect(Base_, guardSize, PROT_NONE);

    Stack_ = Base_ + guardSize;
    YCHECK((reinterpret_cast<uintptr_t>(Stack_)& 15) == 0);
}

TExecutionStack::~TExecutionStack()
{
    const size_t guardSize = GuardPages * GetPageSize();
    ::munmap(Base_, guardSize + Size_);
}

#elif defined(_win_)

TExecutionStack::TExecutionStack(size_t size)
    : TExecutionStackBase(size)
    , Handle_(::CreateFiber(Size_, &FiberTrampoline, this))
    , Trampoline_(nullptr)
{ }

TExecutionStack::~TExecutionStack()
{
    ::DeleteFiber(Handle_);
}

TLS_STATIC void* FiberTrampolineOpaque;

void TExecutionStack::SetOpaque(void* opaque)
{
    FiberTrampolineOpaque = opaque;
}

void* TExecutionStack::GetOpaque()
{
    return FiberTrampolineOpaque;
}

void TExecutionStack::SetTrampoline(void (*trampoline)(void*))
{
    YASSERT(!Trampoline_);
    Trampoline_ = trampoline;
}

VOID CALLBACK TExecutionStack::FiberTrampoline(PVOID opaque)
{
    auto* stack = reinterpret_cast<TExecutionStack*>(opaque);
    stack->Trampoline_(FiberTrampolineOpaque);
}

#else
#   error Unsupported platform
#endif

////////////////////////////////////////////////////////////////////////////////

template <size_t Size>
class TPooledExecutionStack
    : public TExecutionStack
{
public:
    TPooledExecutionStack()
        : TExecutionStack(Size)
    { }
};

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStack stack)
{
    switch (stack) {
        case EExecutionStack::Small:
            return ObjectPool<TPooledExecutionStack<SmallExecutionStackSize>>().Allocate();
        case EExecutionStack::Large:
            return ObjectPool<TPooledExecutionStack<LargeExecutionStackSize>>().Allocate();
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
struct TPooledObjectTraits<NConcurrency::TPooledExecutionStack<Size>, void>
    : public TPooledObjectTraitsBase
{
    typedef NConcurrency::TPooledExecutionStack<Size> TStack;

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
#if defined(_unix_)
        return 1024;
#elif defined(_win_)
        return 0;
#endif
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

