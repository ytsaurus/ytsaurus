#pragma once

#include "cellar_manager.h"
#include "occupant.h"
#include "public.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellar
    : public TRefCounted
{
    virtual void Initialize() = 0;
    virtual void Reconfigure(const TDynamicCellarConfigPtr& config) = 0;

    virtual int GetAvailableSlotCount() const = 0;
    virtual int GetOccupantCount() const = 0;
    virtual const std::vector<ICellarOccupantPtr>& Occupants() const = 0;
    virtual ICellarOccupantPtr FindOccupant(NElection::TCellId id) const = 0;
    virtual ICellarOccupantPtr GetOccupantOrCrash(NElection::TCellId id) const = 0;

    virtual void CreateOccupant(const NTabletClient::NProto::TCreateTabletSlotInfo& createInfo) = 0;
    virtual void ConfigureOccupant(
        const ICellarOccupantPtr& occupant,
        const NTabletClient::NProto::TConfigureTabletSlotInfo& configureInfo) = 0;
    virtual TFuture<void> RemoveOccupant(const ICellarOccupantPtr& occupant) = 0;

    virtual void RegisterOccupierProvider(ICellarOccupierProviderPtr provider) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellar)

ICellarPtr CreateCellar(
    TCellarConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent

