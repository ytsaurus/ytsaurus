#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NQueryTracker {

///////////////////////////////////////////////////////////////////////////////

struct IQueryTracker
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void OnDynamicConfigChanged(const TQueryTrackerDynamicConfigPtr& config) = 0;

    virtual void PopulateAlerts(std::vector<TError>* alerts) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueryTracker)

///////////////////////////////////////////////////////////////////////////////

IQueryTrackerPtr CreateQueryTracker(
    TQueryTrackerDynamicConfigPtr config,
    TString selfAddress,
    IInvokerPtr controlInvoker,
    NApi::IClientPtr stateClient,
    NYPath::TYPath stateRoot,
    int minRequiredStateVersion);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
