#include "scheduler_base.h"
#include "private.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerThreadBase::TSchedulerThreadBase(
    TIntrusivePtr<TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName,
    int shutdownPriority)
    : TThread(threadName, shutdownPriority)
    , CallbackEventCount_(std::move(callbackEventCount))
    , ThreadGroupName_(threadGroupName)
    , ThreadName_(threadName)
{ }

TSchedulerThreadBase::~TSchedulerThreadBase()
{
    Stop();
}

void TSchedulerThreadBase::OnStart()
{ }

void TSchedulerThreadBase::OnStop()
{ }

void TSchedulerThreadBase::Stop(bool graceful)
{
    GracefulStop_.store(graceful);
    TThread::Stop();
}

void TSchedulerThreadBase::StartEpilogue()
{
    OnStart();
}

void TSchedulerThreadBase::StopPrologue()
{
    CallbackEventCount_->NotifyAll();
}

void TSchedulerThreadBase::StopEpilogue()
{
    OnStop();
}

void TSchedulerThreadBase::ThreadMain()
{
    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        YT_LOG_DEBUG("Thread started (Name: %v)",
            ThreadName_);

        while (true) {
            if (IsStopping() && !GracefulStop_.load()) {
                break;
            }

            auto cookie = CallbackEventCount_->PrepareWait();
            if (OnLoop(&cookie)) {
                continue;
            }

            if (IsStopping() && !GracefulStop_.load()) {
                break;
            }
            if (!CallbackEventCount_->Wait(cookie, IsStopping() ? TDuration::Zero() : TDuration::Max())) {
                break;
            }
        }

        YT_LOG_DEBUG("Thread stopped (Name: %v)",
            ThreadName_);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)",
            ThreadName_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
