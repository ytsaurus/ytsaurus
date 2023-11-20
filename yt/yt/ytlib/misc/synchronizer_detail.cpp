#include "synchronizer_detail.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSynchronizerBase::TSynchronizerBase(
    IInvokerPtr invoker,
    TPeriodicExecutorOptions options,
    NLogging::TLogger logger)
    : Logger(std::move(logger))
    , SyncExecutor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND(&TSynchronizerBase::OnSync, MakeWeak(this)),
        options))
{ }

void TSynchronizerBase::Start()
{
    YT_UNUSED_FUTURE(DoStart(Guard(SpinLock_), /*syncImmediately*/ false));
}

void TSynchronizerBase::Stop()
{
    TPromise<void> promise;
    {
        auto guard = Guard(SpinLock_);
        if (std::exchange(Stopped_, true)) {
            return;
        }
        promise = SyncPromise_;
    }

    YT_UNUSED_FUTURE(SyncExecutor_->Stop());
    promise.Set(TError("Synchronizer is stopped"));
}

TFuture<void> TSynchronizerBase::Sync(bool immediately)
{
    auto guard = Guard(SpinLock_);
    if (Stopped_) {
        return SyncPromise_.ToFuture();
    }
    return DoStart(std::move(guard), immediately);
}

TFuture<void> TSynchronizerBase::DoStart(TGuard<NThreading::TSpinLock>&& guard, bool syncImmediately)
{
    YT_VERIFY(!Stopped_);

    auto future = SyncPromise_.ToFuture();
    auto wasStarted = std::exchange(Started_, true);

    guard.Release();

    if (!wasStarted) {
        SyncExecutor_->Start();
    }
    if (!wasStarted || syncImmediately) {
        SyncExecutor_->ScheduleOutOfBand();
    }

    return future;
}

void TSynchronizerBase::OnSync()
{
    TPromise<void> promise;
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return;
        }
        promise = std::exchange(SyncPromise_, NewPromise<void>());
    }

    try {
        DoSync();
        promise.Set();
    } catch (const std::exception& ex) {
        TError error(ex);
        YT_LOG_DEBUG(error);
        promise.Set(std::move(error));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
