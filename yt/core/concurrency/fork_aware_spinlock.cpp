#include "fork_aware_spinlock.h"
#include "rw_spinlock.h"

#include <yt/core/misc/lfalloc_helpers.h>

#ifdef _unix_
    #include <pthread.h>
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static TReaderWriterSpinLock ForkLock;

////////////////////////////////////////////////////////////////////////////////

void TForkAwareSpinLock::Acquire()
{
    ForkLock.AcquireReader();
    SpinLock_.Acquire();
}

void TForkAwareSpinLock::Release()
{
    SpinLock_.Release();
    ForkLock.ReleaseReader();
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

static void BeforeLFAllocGlobalLockAcquired();
static void AfterLFAllocGlobalLockReleased();

class TForkProtector
{
public:
    TForkProtector()
    {
        pthread_atfork(
            &TForkProtector::OnPrepare,
            &TForkProtector::OnParent,
            &TForkProtector::OnChild);
        NLFAlloc::SafeMallocSetParam(
            "BeforeLFAllocGlobalLockAcquired",
            (void*)&BeforeLFAllocGlobalLockAcquired);
        NLFAlloc::SafeMallocSetParam(
            "AfterLFAllocGlobalLockReleased",
            (void*)&AfterLFAllocGlobalLockReleased);
    }

private:
    static void OnPrepare()
    {
        ForkLock.AcquireWriter();
    }

    static void OnParent()
    {
        ForkLock.ReleaseWriter();
    }

    static void OnChild()
    {
        ForkLock.ReleaseWriter();
    }
};

static TForkProtector ForkProtector;

// LFAlloc hooks

static void BeforeLFAllocGlobalLockAcquired()
{
    NYT::NConcurrency::ForkLock.AcquireReader();
}

static void AfterLFAllocGlobalLockReleased()
{
    NYT::NConcurrency::ForkLock.ReleaseReader();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

