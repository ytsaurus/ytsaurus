#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSynchronizerBase
    : public virtual TRefCounted
{
public:
    void Start();
    void Stop();

    TFuture<void> Sync(bool immediately = false);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Stopped_ = false;
    TPromise<void> SyncPromise_ = NewPromise<void>();

    TFuture<void> DoStart(TGuard<NThreading::TSpinLock>&& guard, bool syncImmediately);
    void OnSync();

protected:
    const NLogging::TLogger Logger;
    const NConcurrency::TPeriodicExecutorPtr SyncExecutor_;

    TSynchronizerBase(
        IInvokerPtr invoker,
        NConcurrency::TPeriodicExecutorOptions options,
        NLogging::TLogger logger);

    virtual void DoSync() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
