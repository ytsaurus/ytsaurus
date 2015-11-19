#pragma once

#include "public.h"

#include <core/actions/callback.h>

#include <core/misc/shutdownable.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

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
