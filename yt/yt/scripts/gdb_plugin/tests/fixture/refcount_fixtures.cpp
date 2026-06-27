#include "refcount_fixtures.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/weak_ptr.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <util/datetime/base.h>

#include <util/system/compiler.h>

#include <utility>

using namespace NYT;
using namespace NYT::NConcurrency;

namespace {

// A two-node strong reference cycle: head <-> tail, each holding a strong
// intrusive pointer to the other.
struct TGdbCycleTail;

struct TGdbCycleHead
    : public NYT::TRefCounted
{
    NYT::TIntrusivePtr<TGdbCycleTail> Tail;
};

struct TGdbCycleTail
    : public NYT::TRefCounted
{
    NYT::TIntrusivePtr<TGdbCycleHead> Head;
};

// A standalone live object (no cycle) for the type-name sweep.
struct TGdbLiveSolo
    : public NYT::TRefCounted
{
    int Payload = 0;
};

// A final type that does NOT derive NYT::TRefCounted: New<T> lays the counter right
// before the object (the "final type" machinery). It has a vtable (from the
// non-ref-counted base) so the walker can still name it.
struct IGdbThing
{
    virtual ~IGdbThing() = default;
};

struct TGdbFinalThing final
    : public IGdbThing
{
    int Payload = 0;
};

// Holds its target through a TAtomicIntrusivePtr, which packs a local refcount
// into the pointer's top 16 bits -- so the stored word is not the bare address.
struct TGdbAtomicHolder
    : public NYT::TRefCounted
{
    TAtomicIntrusivePtr<TGdbLiveSolo> Ptr;
};

// Virtual (diamond) inheritance: NYT::TRefCounted is a *shared virtual* base, so its
// sub-object (which carries the counter) sits at a runtime vbase offset and a
// pointer to a base sub-object is an interior pointer (!= the most-derived addr).
struct IGdbLeft
    : public virtual NYT::TRefCounted
{
    virtual void Left()
    { }
};

struct IGdbRight
    : public virtual NYT::TRefCounted
{
    virtual void Right()
    { }
};

struct TGdbDiamond
    : public IGdbLeft
    , public IGdbRight
{
    int Payload = 0;
};

// Weak references must never count as retention. Parent holds Child strongly;
// Child holds Parent *weakly* -- were the walker to mistake the weak edge for a
// strong one it would report a bogus CYCLE instead of a ROOT.
struct TGdbWeakChild;

struct TGdbWeakParent
    : public NYT::TRefCounted
{
    NYT::TIntrusivePtr<TGdbWeakChild> Child;
};

struct TGdbWeakChild
    : public NYT::TRefCounted
{
    TWeakPtr<TGdbWeakParent> Parent;
};

// Root long-lived objects so they survive to the breakpoint.
NYT::TIntrusivePtr<TGdbCycleHead> RootedCycle;
NYT::TIntrusivePtr<TGdbLiveSolo> RootedSolo;
NYT::TIntrusivePtr<TGdbFinalThing> RootedFinal;
NYT::TIntrusivePtr<TGdbAtomicHolder> RootedAtomicHolder;
NYT::TIntrusivePtr<TGdbDiamond> RootedDiamond;
NYT::TRefCountedPtr RootedDiamondAsBase;
NYT::TIntrusivePtr<TGdbWeakParent> RootedWeakParent;
IThreadPoolPtr RootedPool;
TFuture<void> RootedFuture;

} // namespace

// C linkage so the gdb test can name these without mangling; they expose the
// object addresses so the test needs no inferior call against the core.
extern "C" {
void* GdbCycleHeadAddress = nullptr;
void* GdbCycleTailAddress = nullptr;
void* GdbLiveSoloAddress = nullptr;
void* GdbFiberHeldAddress = nullptr;  // pinned only by a parked fiber's stack
void* GdbThreadHeldAddress = nullptr; // pinned only by a running thread's stack
void* GdbFinalAddress = nullptr;      // New<T> final-type (counter-before-object)
void* GdbAtomicHeldAddress = nullptr; // held via TAtomicIntrusivePtr (tagged ptr)
void* GdbDiamondAddress = nullptr;     // most-derived addr of a virtual-diamond object
void* GdbDiamondBaseAddress = nullptr; // its shared virtual NYT::TRefCounted sub-object (interior)
void* GdbWeakParentAddress = nullptr;  // held strongly by a global, weakly by its child
void* GdbWeakChildAddress = nullptr;   // holds the parent only weakly
} // extern "C"

NYT::TRefCountedPtr SetupGdbRefCountFixtures()
{
    // (1) A heap reference cycle.
    auto head = New<TGdbCycleHead>();
    auto tail = New<TGdbCycleTail>();
    head->Tail = tail;
    tail->Head = head; // strong cycle

    auto solo = New<TGdbLiveSolo>();

    RootedCycle = head;
    RootedSolo = solo;
    GdbCycleHeadAddress = head.Get();
    GdbCycleTailAddress = tail.Get();
    GdbLiveSoloAddress = solo.Get();

    // (2) A ref-counted object pinned only by a parked fiber's stack: the fiber
    // blocks in WaitFor on a never-set future, keeping `held` live on its stack.
    RootedPool = CreateThreadPool(1, "GdbPool");
    auto neverSet = NewPromise<void>();
    RootedFuture = BIND([neverSet] {
        auto held = New<TGdbLiveSolo>();
        GdbFiberHeldAddress = held.Get();
        WaitFor(neverSet.ToFuture()).ThrowOnError();
    })
        .AsyncVia(RootedPool->GetInvoker())
        .Run();

    while (!GdbFiberHeldAddress) {
        Sleep(TDuration::MilliSeconds(10));
    }
    Sleep(TDuration::MilliSeconds(200)); // let the fiber reach WaitFor and park

    // (3) New<T> for a final, non-NYT::TRefCounted type (counter-before-object).
    RootedFinal = New<TGdbFinalThing>();
    GdbFinalAddress = RootedFinal.Get();

    // (4) An object held through a TAtomicIntrusivePtr. Acquire() bumps the local
    // refcount packed into the pointer's top bits, so the stored word is the
    // address with a nonzero tag -- a bare-address search would miss it.
    RootedAtomicHolder = New<TGdbAtomicHolder>();
    auto atomicObj = New<TGdbLiveSolo>();
    GdbAtomicHeldAddress = atomicObj.Get();
    RootedAtomicHolder->Ptr.Store(std::move(atomicObj));
    for (int i = 0; i < 3; ++i) {
        Y_UNUSED(RootedAtomicHolder->Ptr.Acquire());
    }

    // (5) Virtual (diamond) inheritance. Hold the same object two ways: via the
    // most-derived pointer and via the shared virtual base (an interior pointer).
    RootedDiamond = New<TGdbDiamond>();
    GdbDiamondAddress = RootedDiamond.Get();
    RootedDiamondAsBase = RootedDiamond; // NYT::TRefCountedPtr stores the vbase pointer
    GdbDiamondBaseAddress = static_cast<NYT::TRefCounted*>(RootedDiamond.Get());

    // (6) Strong parent <-> weak child: a weak edge must not close a cycle.
    RootedWeakParent = New<TGdbWeakParent>();
    auto weakChild = New<TGdbWeakChild>();
    RootedWeakParent->Child = weakChild;  // strong: parent -> child
    weakChild->Parent = RootedWeakParent; // weak:   child -> parent
    GdbWeakParentAddress = RootedWeakParent.Get();
    GdbWeakChildAddress = weakChild.Get();

    // (7) An object pinned only by a running thread's stack. Created last and
    // returned by value: main() keeps the sole strong ref as a local, so nothing
    // on the heap or in a global holds it -- only the running thread's stack does.
    auto onMainStack = New<TGdbLiveSolo>();
    GdbThreadHeldAddress = onMainStack.Get();
    return onMainStack;
}
