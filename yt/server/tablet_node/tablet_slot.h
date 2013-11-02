#pragma once

#include "public.h"

#include <ytlib/hydra/public.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/hydra/public.h>

#include <server/hive/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra managing a number of tablets.
class TTabletSlot
    : public TRefCounted
{
public:
    TTabletSlot(
        int slotIndex,
        NCellNode::TCellNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    
    ~TTabletSlot();

    const NHydra::TCellGuid& GetCellGuid() const;
    NHydra::EPeerState GetState() const;
    NHydra::TPeerId GetPeerId() const;
    const NHydra::NProto::TCellConfig& GetCellConfig() const;
    
    NHydra::IHydraManagerPtr GetHydraManager() const;
    TTabletAutomatonPtr GetAutomaton() const;
    IInvokerPtr GetAutomatonInvoker() const;
    IInvokerPtr GetEpochAutomatonInvoker() const;

    NHive::THiveManagerPtr GetHiveManager() const;
    NHive::TMailbox* GetMasterMailbox();

    TTransactionManagerPtr GetTransactionManager() const;
    NHive::TTransactionSupervisorPtr GetTransactionSupervisor() const;

    TTabletManagerPtr GetTabletManager() const;
    
    void Load(const NHydra::TCellGuid& cellGuid);
    void Create(const NNodeTrackerClient::NProto::TCreateTabletSlotInfo& createInfo);
    void Configure(const NNodeTrackerClient::NProto::TConfigureTabletSlotInfo& configureInfo);
    void Remove();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
