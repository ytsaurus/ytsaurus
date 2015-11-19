#pragma once

#include "public.h"

#include <core/actions/callback.h>

#include <core/misc/shutdownable.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue
    : public TRefCounted
    , public IShutdownable
{
public:
    TFairShareActionQueue(
        const Stroka& threadName,
        const std::vector<Stroka>& bucketNames,
        bool enableLogging = true,
        bool enableProfiling = true);

    virtual ~TFairShareActionQueue();

    virtual void Shutdown() override;

    IInvokerPtr GetInvoker(int index);

    static TCallback<TFairShareActionQueuePtr()> CreateFactory(
        const Stroka& threadName,
        const std::vector<Stroka>& bucketNames,
        bool enableLogging = true,
        bool enableProfiling = true);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TFairShareActionQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
