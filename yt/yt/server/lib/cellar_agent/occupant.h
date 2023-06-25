#pragma once

#include "config.h"
#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/heartbeat.pb.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarOccupant
    : public TRefCounted
{
    virtual ICellarOccupierPtr GetOccupier() const = 0;
    template <typename T>
    TIntrusivePtr<T> GetTypedOccupier() const;

    virtual int GetIndex() const = 0;

    virtual NHydra::TCellId GetCellId() const = 0;
    virtual NHydra::EPeerState GetControlState() const = 0;
    virtual NHydra::EPeerState GetAutomatonState() const = 0;
    virtual NHydra::TPeerId GetPeerId() const = 0;
    virtual const NHiveClient::TCellDescriptor& GetCellDescriptor() const = 0;
    virtual int GetConfigVersion() const = 0;

    virtual const NHydra::IDistributedHydraManagerPtr GetHydraManager() const = 0;
    virtual const NRpc::IResponseKeeperPtr& GetResponseKeeper() const = 0;
    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() const = 0;
    virtual const NHiveServer::IHiveManagerPtr& GetHiveManager() const = 0;
    virtual const NHiveServer::TSimpleAvenueDirectoryPtr& GetAvenueDirectory() const = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const = 0;
    virtual const NTransactionSupervisor::ITransactionSupervisorPtr& GetTransactionSupervisor() const = 0;
    virtual NHiveServer::TMailbox* GetMasterMailbox() const = 0;
    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) const = 0;

    virtual void Initialize() = 0;

    virtual bool CanConfigure() const = 0;
    virtual void Configure(const NCellarNodeTrackerClient::NProto::TConfigureCellSlotInfo& configureInfo) = 0;

    virtual int GetDynamicConfigVersion() const = 0;
    virtual void UpdateDynamicConfig(const NCellarNodeTrackerClient::NProto::TUpdateCellSlotInfo& updateInfo) = 0;
    virtual void Reconfigure(NHydra::TDynamicDistributedHydraManagerConfigPtr config) = 0;

    virtual TFuture<void> Finalize() = 0;

    virtual const NYTree::IYPathServicePtr& GetOrchidService() const = 0;

    virtual const TString& GetCellBundleName() const = 0;

    virtual NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() const = 0;
    virtual const NTabletClient::TTabletCellOptionsPtr& GetOptions() const = 0;

    virtual void PopulateAlerts(std::vector<TError>* alerts) const = 0;

    virtual const IInvokerPtr& GetSnapshotLocalIOInvoker() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarOccupant)

ICellarOccupantPtr CreateCellarOccupant(
    int index,
    TCellarOccupantConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap,
    const NCellarNodeTrackerClient::NProto::TCreateCellSlotInfo& createInfo,
    ICellarOccupierPtr occupier);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent

#define OCCUPANT_INL_H_
#include "occupant-inl.h"
#undef OCCUPANT_INL_H_
