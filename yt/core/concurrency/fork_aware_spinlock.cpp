#include "stdafx.h"
#include "fork_aware_spinlock.h"
#include "rw_spinlock.h"

#ifdef _unix_
    // fork()
    #include <sys/types.h>
    #include <unistd.h>
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

pid_t SafeFork()
{
#ifdef _unix_
    ForkLock.AcquireWriter();
    auto result = ::fork();
    ForkLock.ReleaseWriter();
    return result;
#else
    YUNREACAHBLE();
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

