#include "stdafx.h"
#include "fork_aware_spinlock.h"
#include "rw_spinlock.h"

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

pid_t SafeFork()
{
    ForkLock.AcquireWriter();
    auto result = ::fork();
    ForkLock.ReleaseWriter();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

