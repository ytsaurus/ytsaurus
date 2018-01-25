#pragma once

#ifndef __linux__
#error Platform must be linux to include this
#endif

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
    virtual IInstancePtr CreateInstance(bool autoDestroy = true) = 0;
    virtual IInstancePtr GetSelfInstance() = 0;
    virtual IInstancePtr GetInstance(const TString& name) = 0;
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
    const TNullable<TString>& rootContainer,
    TCallback<void(const TError&)> errorHandler,
    const TPortoManagerConfig& portoManagerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT
