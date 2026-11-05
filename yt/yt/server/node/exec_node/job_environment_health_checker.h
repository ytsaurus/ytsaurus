#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IJobEnvironmentHealthChecker
    : public virtual TRefCounted
{
    virtual void Start(TClosure onSuccess, TCallback<void(const TError&)> onFailed) = 0;

    virtual void OnDynamicConfigChanged(const TSlotManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobEnvironmentHealthChecker)

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentHealthCheckerPtr CreateJobEnvironmentHealthChecker(TNonNullPtr<IBootstrap> bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
