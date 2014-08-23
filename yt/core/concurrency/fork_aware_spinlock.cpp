#include "stdafx.h"
#include "fork_aware_spinlock.h"
#include "rw_spinlock.h"

#ifdef _unix_
    #include <pthread.h>
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static TReaderWriterSpinLock ForkLock;

////////////////////////////////////////////////////////////////////////////////

class TForkAwareSpinLock::TImpl
{
public:
    void Acquire()
    {
        ForkLock.AcquireReader();
        SpinLock_.Acquire();
    }

    void Release()
    {
        ForkLock.ReleaseReader();
        SpinLock_.Release();
    }

private:
    TSpinLock SpinLock_;

};

////////////////////////////////////////////////////////////////////////////////

TForkAwareSpinLock::TForkAwareSpinLock()
    : Impl_(new TImpl())
{ }

TForkAwareSpinLock::~TForkAwareSpinLock()
{ }

void TForkAwareSpinLock::Acquire()
{
    Impl_->Acquire();
}

void TForkAwareSpinLock::Release()
{
    Impl_->Release();
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

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

