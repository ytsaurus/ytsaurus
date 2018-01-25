#include "execution_stack.h"
#include "execution_context.h"
#include "fiber.h"

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/ref_tracked.h>

#if defined(_unix_)
#   include <sys/mman.h>
#   include <limits.h>
#   include <unistd.h>
#   if !defined(__x86_64__)
#       error Unsupported platform
#   endif
#endif

#include <yt/core/misc/error.h>
#include <yt/core/misc/object_pool.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Stack sizes.
static constexpr size_t SmallExecutionStackSize = 256_KB;
static constexpr size_t LargeExecutionStackSize = 8_MB;

////////////////////////////////////////////////////////////////////////////////

TExecutionStackBase::TExecutionStackBase(size_t size)
    : Stack_(nullptr)
    , Size_(RoundUpToPage(size))
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
    const size_t guardSize = GuardPageCount * GetPageSize();

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
    const size_t guardSize = GuardPageCount * GetPageSize();
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

static PER_THREAD void* FiberTrampolineOpaque;

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
    Y_ASSERT(!Trampoline_);
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

template <EExecutionStackKind Kind, size_t Size>
class TPooledExecutionStack
    : public TExecutionStack
    , public TRefTracked<TPooledExecutionStack<Kind, Size>>
{
public:
    TPooledExecutionStack()
        : TExecutionStack(Size)
    { }
};

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStackKind kind)
{
    switch (kind) {
        case EExecutionStackKind::Small:
            return ObjectPool<TPooledExecutionStack<EExecutionStackKind::Small, SmallExecutionStackSize>>().Allocate();
        case EExecutionStackKind::Large:
            return ObjectPool<TPooledExecutionStack<EExecutionStackKind::Large, LargeExecutionStackSize>>().Allocate();
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <NConcurrency::EExecutionStackKind Kind, size_t Size>
struct TPooledObjectTraits<NConcurrency::TPooledExecutionStack<Kind, Size>, void>
    : public TPooledObjectTraitsBase
{
    typedef NConcurrency::TPooledExecutionStack<Kind, Size> TStack;

    static void Clean(TStack* stack)
    {
#if !defined(NDEBUG) && !defined(_asan_enabled_)
        if (stack->GetStack()) {
            memset(stack->GetStack(), 0, stack->GetSize());
        }
#endif
#if defined(_asan_enabled_)
        if (stack->GetStack()) {
            NSan::Poison(stack->GetStack(), stack->GetSize());
        }
#endif
    }

    static int GetMaxPoolSize()
    {
        return NConcurrency::GetFiberStackPoolSize(Kind);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

