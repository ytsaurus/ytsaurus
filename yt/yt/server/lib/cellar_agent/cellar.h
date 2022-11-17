#pragma once

#include "cellar_manager.h"
#include "occupant.h"
#include "public.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/heartbeat.pb.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellar
    : public TRefCounted
{
    DECLARE_INTERFACE_SIGNAL(void(), CreateOccupant);
    DECLARE_INTERFACE_SIGNAL(void(), RemoveOccupant);
    DECLARE_INTERFACE_SIGNAL(void(), UpdateOccupant);

    virtual void Initialize() = 0;
    virtual void Reconfigure(const TCellarDynamicConfigPtr& config) = 0;

    virtual int GetAvailableSlotCount() const = 0;
    virtual int GetOccupantCount() const = 0;
    virtual const std::vector<ICellarOccupantPtr>& Occupants() const = 0;
    virtual ICellarOccupantPtr FindOccupant(NElection::TCellId id) const = 0;
    virtual ICellarOccupantPtr GetOccupant(NElection::TCellId id) const = 0;

    virtual void CreateOccupant(const NCellarNodeTrackerClient::NProto::TCreateCellSlotInfo& createInfo) = 0;
    virtual void ConfigureOccupant(
        const ICellarOccupantPtr& occupant,
        const NCellarNodeTrackerClient::NProto::TConfigureCellSlotInfo& configureInfo) = 0;
    virtual void UpdateOccupant(
        const ICellarOccupantPtr& occupant,
        const NCellarNodeTrackerClient::NProto::TUpdateCellSlotInfo& updateInfo) = 0;
    virtual TFuture<void> RemoveOccupant(const ICellarOccupantPtr& occupant) = 0;

    virtual void RegisterOccupierProvider(ICellarOccupierProviderPtr provider) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void PopulateAlerts(std::vector<TError>* error) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellar)

ICellarPtr CreateCellar(
    NCellarClient::ECellarType type,
    TCellarConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent

