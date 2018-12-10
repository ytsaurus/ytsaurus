#include "fork_aware_spinlock.h"
#include "rw_spinlock.h"

#ifdef _unix_
    #include <pthread.h>
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TReaderWriterSpinLock& ForkLock()
{
    static TReaderWriterSpinLock lock;
    return lock;
}

////////////////////////////////////////////////////////////////////////////////

void TForkAwareSpinLock::Acquire()
{
    ForkLock().AcquireReader();
    SpinLock_.Acquire();
}

void TForkAwareSpinLock::Release()
{
    SpinLock_.Release();
    ForkLock().ReleaseReader();
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

class TForkProtector
{
public:
    TForkProtector()
    {
        pthread_atfork(
            &TForkProtector::OnPrepare,
            &TForkProtector::OnParent,
            &TForkProtector::OnChild);
    }

private:
    static void OnPrepare()
    {
        ForkLock().AcquireWriter();
    }

    static void OnParent()
    {
        ForkLock().ReleaseWriter();
    }

    static void OnChild()
    {
        ForkLock().ReleaseWriter();
    }
};

static TForkProtector ForkProtector;

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

