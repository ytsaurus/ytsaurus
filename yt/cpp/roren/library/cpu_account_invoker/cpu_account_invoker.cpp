#include "cpu_account_invoker.h"

namespace NRoren
{

////////////////////////////////////////////////////////////////////////////////

i64 ClockGetTime(TClockId clockId)
{
    constexpr i64 setToNsecMultiplier = 1000 * 1000 * 1000;
    timespec value;
    int ret = clock_gettime(clockId, &value);
    if (ret != 0) {
        THROW_ERROR_EXCEPTION("ClockGetTime() error");
    }
    return static_cast<i64>(value.tv_sec) * setToNsecMultiplier + static_cast<i64>(value.tv_nsec);
}

////////////////////////////////////////////////////////////////////////////////

TAccountCpuTimeInvoker::TFiberCpuTimer::TFiberCpuTimer(NYT::TIntrusivePtr<TSharedState> sharedState)
    : NYT::NConcurrency::TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Start(); })
    , SharedState_(std::move(sharedState))
{
    Start();
}

TAccountCpuTimeInvoker::TFiberCpuTimer::~TFiberCpuTimer()
{
    Stop();
}

void TAccountCpuTimeInvoker::TFiberCpuTimer::Start()
{
    Active_ = true;
    StartTime_ = ClockGetTime(SharedState_->ClockId);
}

void TAccountCpuTimeInvoker::TFiberCpuTimer::Stop()
{
    if (!Active_) {
        return;
    }

    Active_ = false;
    SharedState_->DurationNSec_ += ClockGetTime(SharedState_->ClockId) - StartTime_;
    StartTime_ = 0;
}

TAccountCpuTimeInvoker::TAccountCpuTimeInvoker(NYT::IInvokerPtr invoker)
    : Invoker_(std::move(invoker))
    , SharedState_(NYT::New<TFiberCpuTimer::TSharedState>())
{
}

TDuration TAccountCpuTimeInvoker::GetCpuTime() const
{
    return TDuration::MicroSeconds(SharedState_->DurationNSec_ / 1000);
}

NYT::TClosure TAccountCpuTimeInvoker::MakeWrappedCallback(NYT::TClosure callback)
{
    return BIND_NO_PROPAGATE([sharedState = SharedState_, callback = std::move(callback)] () mutable {
        TFiberCpuTimer guard(std::move(sharedState));
        callback();
    });
}

void TAccountCpuTimeInvoker::Invoke(NYT::TClosure callback)
{
    Invoker_->Invoke(MakeWrappedCallback(std::move(callback)));
}

void TAccountCpuTimeInvoker::Invoke(NYT::TMutableRange<NYT::TClosure> callbacks)
{
    std::transform(
        callbacks.Begin(),
        callbacks.End(),
        callbacks.Begin(),
        [this] (NYT::TClosure& callback) { return this->MakeWrappedCallback(std::move(callback)); }
    );
    Invoker_->Invoke(std::move(callbacks));
}

NYT::NThreading::TThreadId TAccountCpuTimeInvoker::GetThreadId() const
{
    return Invoker_->GetThreadId();
}

bool TAccountCpuTimeInvoker::CheckAffinity(const NYT::IInvokerPtr& invoker) const
{
    return Invoker_->CheckAffinity(invoker);
}

bool TAccountCpuTimeInvoker::IsSerialized() const
{
    return Invoker_->IsSerialized();
}

void TAccountCpuTimeInvoker::RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver)
{
    return Invoker_->RegisterWaitTimeObserver(std::move(waitTimeObserver));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren

