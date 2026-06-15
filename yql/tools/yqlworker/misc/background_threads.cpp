#include "background_threads.h"


namespace NYql {

TBackgroundThreads::TBackgroundThreads()
    : KeepRunning_(1)
{
}

TBackgroundThreads::~TBackgroundThreads()
{
    Stop();
}

void TBackgroundThreads::Stop()
{
    // do nothing if this is called twice
    if (AtomicSwap(&KeepRunning_, 0)) {
        for (auto& t : Threads_) {
            t.join();
        }
    }
}

void TBackgroundThreads::Add(TCallable c, TDuration initialSleep)
{
    auto method = &TBackgroundThreads::ExecuteInLoop;
    Threads_.emplace_back(method, this, c, initialSleep);
}

void TBackgroundThreads::ExecuteInLoop(TCallable c, TDuration initialSleep) const
{
    IncrementalSleep(initialSleep);
    while (AtomicGet(KeepRunning_)) {
        TDuration sleepTime = c();
        if (sleepTime == TDuration::Max()) {
            break;
        }
        IncrementalSleep(sleepTime);
    }
}

void TBackgroundThreads::IncrementalSleep(TDuration amount) const
{
    static const TDuration ONE_SECOND = TDuration::Seconds(1);

    while (amount > TDuration::Zero() && AtomicGet(KeepRunning_)) {
        auto s = amount > ONE_SECOND ? ONE_SECOND : amount;
        Sleep(s);
        amount -= s;
    }
}

} // namespace NYql
