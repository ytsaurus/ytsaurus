#include "scheduler_base.h"
#include "private.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TAction>
void CheckedAction(std::atomic<ui64>* atomicEpoch, ui64 mask, TAction&& action)
{
    bool alreadyDone = false;
    ui64 epoch;
    while (true) {
        epoch = atomicEpoch->load(std::memory_order_acquire);
        if (epoch & mask) {
            // Action requested; await.
            alreadyDone = true;
            break;
        }
        if (atomicEpoch->compare_exchange_strong(epoch, epoch | mask, std::memory_order_release)) {
            break;
        }
    }

    if (!alreadyDone) {
        action(epoch);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSchedulerThreadBase::TSchedulerThreadBase(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName)
    : CallbackEventCount_(std::move(callbackEventCount))
    , ThreadName_(threadName)
    , Thread_(ThreadTrampoline, (void*) this)
{ }

TSchedulerThreadBase::~TSchedulerThreadBase()
{
    Shutdown();
}

void TSchedulerThreadBase::OnStart()
{ }

void TSchedulerThreadBase::BeforeShutdown()
{ }

void TSchedulerThreadBase::AfterShutdown()
{ }

void TSchedulerThreadBase::OnThreadStart()
{ }

void TSchedulerThreadBase::OnThreadShutdown()
{ }

TThreadId TSchedulerThreadBase::GetId() const
{
    return ThreadId_;
}

bool TSchedulerThreadBase::IsStarted() const
{
    return Epoch_.load(std::memory_order_relaxed) & StartingEpochMask;
}

bool TSchedulerThreadBase::IsShutdown() const
{
    return Epoch_.load(std::memory_order_relaxed) & StoppingEpochMask;
}

void* TSchedulerThreadBase::ThreadTrampoline(void* opaque)
{
    static_cast<TSchedulerThreadBase*>(opaque)->ThreadMain();
    return nullptr;
}

void TSchedulerThreadBase::Start()
{
    CheckedAction(&Epoch_, StartingEpochMask, [&] (ui64 epoch) {
        if (!(epoch & StoppingEpochMask)) {
            try {
                Thread_.Start();
            } catch (const std::exception& ex) {
                fprintf(stderr, "Error starting %s thread\n%s\n",
                    ThreadName_.c_str(),
                    ex.what());

                YT_ABORT();
            }

            ThreadId_ = static_cast<TThreadId>(Thread_.Id());

            OnStart();
        } else {
            // Pretend that thread was started and (immediately) stopped.
            ThreadStartedEvent_.NotifyAll();
        }
    });
}

void TSchedulerThreadBase::Shutdown()
{
    CheckedAction(&Epoch_, StoppingEpochMask, [&] (ui64 epoch) {
        if (epoch & StartingEpochMask) {
            // There is a tiny chance that thread is not started yet, and call to TThread::Join may fail
            // in this case. Ensure proper event sequencing by synchronizing with thread startup.
            ThreadStartedEvent_.Wait();

            CallbackEventCount_->NotifyAll();

            BeforeShutdown();

            // Avoid deadlock.
            if (TThread::CurrentThreadId() == ThreadId_) {
                Thread_.Detach();
            } else {
                Thread_.Join();
            }

            AfterShutdown();
        } else {
            // Thread was not started at all.
        }

        ThreadShutdownEvent_.NotifyAll();
    });

    ThreadShutdownEvent_.Wait();
}

void TSchedulerThreadBase::ThreadMain()
{
    TThread::SetCurrentThreadName(ThreadName_.c_str());

    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        OnThreadStart();
        YT_LOG_DEBUG("Thread started (Name: %v)", ThreadName_);

        ThreadStartedEvent_.NotifyAll();

        while (!IsShutdown()) {
            auto cookie = CallbackEventCount_->PrepareWait();

            if (OnLoop(&cookie)) {
                continue;
            }

            if (IsShutdown()) {
                break;
            }

            CallbackEventCount_->Wait(cookie);
        }

        OnThreadShutdown();
        YT_LOG_DEBUG("Thread stopped (Name: %v)", ThreadName_);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)", ThreadName_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
