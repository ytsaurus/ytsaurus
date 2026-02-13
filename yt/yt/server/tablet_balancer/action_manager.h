#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

//! Mostly not thread-safe. Marked methods should be called in the control invoker.
//! More, this invoker must be serialized.
struct IActionManager
    : public TRefCounted
{
    // Thread affinity: any.
    virtual void ScheduleActionCreation(const std::string& bundleName, const TActionDescriptor& descriptor) = 0;
    // Thread affinity: Control.
    virtual void CreateActions(const std::string& bundleName) = 0;

    // Thread affinity: Control.
    virtual bool HasUnfinishedActions(
        const std::string& bundleName,
        const std::vector<TTabletActionId>& knownBundleActionIds) const = 0;

    // Thread affinity: Control.
    virtual bool HasPendingActions(const std::string& bundleName) const = 0;
    // Thread affinity: Control.
    virtual void CancelPendingActions(const std::string& bundleName) = 0;

    // Thread affinity: Control.
    virtual void Start(NTransactionClient::TTransactionId prerequisiteTransactionId) = 0;
    // Thread affinity: Control.
    virtual void Stop() = 0;

    // Thread affinity: Control.
    virtual void Reconfigure(const TActionManagerConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IActionManager)

////////////////////////////////////////////////////////////////////////////////

IActionManagerPtr CreateActionManager(
    TActionManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IBootstrap* bootstrap,
    IMulticellThrottlerPtr throttler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
