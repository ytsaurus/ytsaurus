#pragma once

#include "public.h"
#include <yt/core/actions/callback.h>
#include <yt/core/actions/future.h>

namespace NYT {
namespace NContainers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECleanMode,
    (None)
    (Dead)
    (All)
);

////////////////////////////////////////////////////////////////////////////////

struct IContainerManager
    : public TRefCounted
{
    virtual IInstancePtr CreateInstance() = 0;
    virtual IInstancePtr GetSelfInstance() = 0;
    virtual TFuture<std::vector<TString>> GetInstanceNames() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IContainerManager)

////////////////////////////////////////////////////////////////////////////////

struct TPortoManagerConfig
{
    const ECleanMode CleanMode;
    const TDuration RetryTime;
    const TDuration PollPeriod;
};

////////////////////////////////////////////////////////////////////////////////

IContainerManagerPtr CreatePortoManager(
    const TString& prefix,
    TCallback<void(const TError&)> errorHandler,
    const TPortoManagerConfig& portoManagerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT
