#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/shutdownable.h>
#include <yt/yt/core/misc/range.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): Facade does not have to be ref-counted.
class TFairShareActionQueue
    : public TRefCounted
    , public IShutdownable
{
public:
    TFairShareActionQueue(
        const TString& threadName,
        const std::vector<TString>& bucketNames);
    TFairShareActionQueue(
        const TString& threadName,
        TRange<TStringBuf> bucketNames);

    virtual ~TFairShareActionQueue();

    virtual void Shutdown() override;

    const IInvokerPtr& GetInvoker(int index);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TFairShareActionQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
