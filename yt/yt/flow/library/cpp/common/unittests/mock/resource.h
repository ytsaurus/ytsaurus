#pragma once

#include <yt/yt/flow/library/cpp/common/resource.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TResourceMockBase
    : public IResource
{
public:
    TResourceMockBase();
    ~TResourceMockBase() override;

    const TCancelableContextPtr& GetCancelableContext() const final;
    TResourceStatus GetStatus() const final;
    TFuture<void> GetDestroyedFuture() const final;

private:
    const TPromise<void> DestroyedPromise_;
    const TFinallyGuard<TClosure> DestroyedGuard_;
    const TCancelableContextPtr CancelableContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
