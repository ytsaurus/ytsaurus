#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): Facade does not have to be ref-counted.
class TThreadPool
    : public TRefCounted
{
public:
    TThreadPool(
        int threadCount,
        const TString& threadNamePrefix,
        bool startThreads = true);

    virtual ~TThreadPool();

    void Shutdown();

    //! Returns current thread count, it can differ from value set by Configure()
    //! because it clamped between 1 and maximum thread count.
    int GetThreadCount();
    void Configure(int threadCount);

    //! Starts the threads if they were not previously started.
    void EnsureStarted();

    const IInvokerPtr& GetInvoker();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TThreadPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
