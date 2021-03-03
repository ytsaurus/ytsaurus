#pragma once

#include "config.h"
#include "public.h"

#include <yt/ytlib/tablet_client/config.h>
#include <yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/client/object_client/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarOccupant
    : public TRefCounted
{
    virtual const ICellarOccupierPtr& GetOccupier() const = 0;
    template <typename T>
    TIntrusivePtr<T> GetTypedOccupier() const;

    virtual int GetIndex() const = 0;

    virtual NHydra::TCellId GetCellId() const = 0;
    virtual NHydra::EPeerState GetControlState() const = 0;
    virtual NHydra::EPeerState GetAutomatonState() const = 0;
    virtual NHydra::TPeerId GetPeerId() const = 0;
    virtual const NHiveClient::TCellDescriptor& GetCellDescriptor() const = 0;

    virtual const NHydra::IDistributedHydraManagerPtr GetHydraManager() const = 0;
    virtual const NRpc::TResponseKeeperPtr& GetResponseKeeper() const = 0;
    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() const = 0;
    virtual const NHiveServer::THiveManagerPtr& GetHiveManager() const = 0;
    virtual const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() const = 0;
    virtual NHiveServer::TMailbox* GetMasterMailbox() const = 0;
    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) const = 0; 

    virtual void Initialize() = 0;
    virtual bool CanConfigure() const = 0;
    virtual void Configure(const NTabletClient::NProto::TConfigureTabletSlotInfo& configureInfo) = 0;
    virtual TFuture<void> Finalize() = 0;

    virtual const NYTree::IYPathServicePtr& GetOrchidService() const = 0;

    virtual const TString& GetCellBundleName() const = 0;

    virtual const NTabletClient::TTabletCellOptionsPtr& GetOptions() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarOccupant)

ICellarOccupantPtr CreateCellarOccupant(
    int index,
    TCellarOccupantConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap,
    const NTabletClient::NProto::TCreateTabletSlotInfo& createInfo,
    ICellarOccupierPtr occupier);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent

#define OCCUPANT_INL_H_
#include "occupant-inl.h"
#undef OCCUPANT_INL_H_
