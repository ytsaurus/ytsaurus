#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

//! Not thread-safe. All methods should be called in the control invoker.
//! More, this invoker must be serialized.
struct IActionManager
    : public TRefCounted
{
    virtual void ScheduleActionCreation(const std::string& bundleName, const TActionDescriptor& descriptor) = 0;
    virtual void CreateActions(const std::string& bundleName) = 0;

    virtual bool HasUnfinishedActions(const std::string& bundleName) const = 0;
    virtual bool IsKnownAction(const std::string& bundleName, TTabletActionId actionId) const = 0;

    virtual bool HasPendingActions(const std::string& bundleName) const = 0;
    virtual void CancelPendingActions(const std::string& bundleName) = 0;

    virtual void Start(NTransactionClient::TTransactionId prerequisiteTransactionId) = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(const TActionManagerConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IActionManager)

////////////////////////////////////////////////////////////////////////////////

IActionManagerPtr CreateActionManager(
    TActionManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
