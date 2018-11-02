#include "fls.h"
#include "atomic_flag_spinlock.h"
#include "fiber.h"
#include "scheduler.h"

#ifdef _unix_
    #include <pthread.h>
#endif

namespace NYT {
namespace NConcurrency {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TFlsSlot
{
    TFlsSlotCtor Ctor;
    TFlsSlotDtor Dtor;
};

static const int FlsMaxSize = 256;
static std::atomic<int> FlsSize(0);

static std::atomic_flag FlsLock = ATOMIC_FLAG_INIT;
static TFlsSlot FlsSlots[FlsMaxSize] = {};

// Thread-specific storage implementation.
// For native threads we use native TLS to store FLS.
// TODO(sandello): Register a destructor on the thread termination.
#if defined(_unix_)
typedef pthread_key_t TTlsKey;
#elif defined(_win_)
typedef DWORD TTlsKey;
#else
#error Unsupported platform
#endif

static TTlsKey FlsTsdKey;

static void TsdDestroy(void* opaque)
{
    uintptr_t* tsd = static_cast<uintptr_t*>(opaque);
    if (tsd) {
        for (int i = 0; i < FlsMaxSize; ++i) {
            if (tsd[i]) {
                FlsDestruct(i, tsd[i]);
            }
        }
        delete[] tsd;
    }
}

static void TsdCreate()
{
#if defined(_unix_)
    YCHECK(pthread_key_create(&FlsTsdKey, &TsdDestroy) == 0);
#elif defined(_win_)
    YCHECK((FlsTsdKey = TlsAlloc()) != TLS_OUT_OF_INDEXES);
#endif
}

static uintptr_t& TsdAt(int index)
{
#if defined(_unix_)
#define TLS_GET_ pthread_getspecific
#define TLS_SET_ !pthread_setspecific
#elif defined(_win_)
#define TLS_GET_ TlsGetValue
#define TLS_SET_ TlsSetValue
#endif
    uintptr_t* tsd = static_cast<uintptr_t*>(TLS_GET_(FlsTsdKey));
    if (Y_LIKELY(tsd)) {
        return tsd[index];
    }

    TMemoryTagGuard guard(NullMemoryTag);
    tsd = new uintptr_t[FlsMaxSize];
    YCHECK(tsd);
    memset(tsd, 0, FlsMaxSize * sizeof(uintptr_t));
    YCHECK(TLS_SET_(FlsTsdKey, tsd));

    return tsd[index];
#undef TLS_GET_
#undef TLS_SET_
}

int FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor)
{
    TGuard<std::atomic_flag> guard(FlsLock);

    int index = FlsSize++;
    YCHECK(index < FlsMaxSize);

    if (index == 0) {
        TsdCreate();
    }

    auto& slot = FlsSlots[index];
    slot.Ctor = ctor;
    slot.Dtor = dtor;

    return index;
}

int FlsCountSlots()
{
    return FlsSize;
}

uintptr_t FlsConstruct(int index)
{
    TMemoryTagGuard guard(NullMemoryTag);
    return FlsSlots[index].Ctor();
}

void FlsDestruct(int index, uintptr_t value)
{
    FlsSlots[index].Dtor(value);
}

uintptr_t& FlsAt(int index, TFiber* fiber)
{
    if (!fiber) {
        if (auto* scheduler = TryGetCurrentScheduler()) {
            fiber = scheduler->GetCurrentFiber();
        }
    }
    if (fiber) {
        return fiber->FsdAt(index);
    }
    return TsdAt(index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NConcurrency
} // namespace NYT

