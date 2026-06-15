#pragma once

#include <functional>
#include <thread>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>


namespace NYql {

class TBackgroundThreads
{
    using TCallable = std::function<TDuration()>;

public:
    TBackgroundThreads();
    virtual ~TBackgroundThreads();

    void Stop();

    void Add(TCallable c, TDuration initialSleep = TDuration::Zero());

    size_t Size() const {
        return Threads_.size();
    }

private:
    void ExecuteInLoop(TCallable c, TDuration initialSleep) const;

    void IncrementalSleep(TDuration amount) const;

private:
    TAtomic KeepRunning_;
    TVector<std::thread> Threads_;
};

} // namespace NYql
