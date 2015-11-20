#pragma once

#include "public.h"

#include <core/actions/callback.h>

#include <core/misc/shutdownable.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): Facade does not have to be ref-counted.
class TThreadPool
    : public TRefCounted
    , public IShutdownable
{
public:
    TThreadPool(
        int threadCount,
        const Stroka& threadNamePrefix,
        bool enableLogging = true,
        bool enableProfiling = true);

    virtual ~TThreadPool();

    virtual void Shutdown() override;

    void Configure(int threadCount);

    IInvokerPtr GetInvoker();

    static TCallback<TThreadPoolPtr()> CreateFactory(
        int threadCount,
        const Stroka& threadName,
        bool enableLogging = true,
        bool enableProfiling = true);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

DEFINE_REFCOUNTED_TYPE(TThreadPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
