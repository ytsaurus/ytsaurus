#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <ytlib/object_client/public.h>

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
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    
    ~TTabletSlot();

    int GetIndex() const;
    const NHydra::TCellId& GetCellId() const;
    NHydra::EPeerState GetControlState() const;
    NHydra::EPeerState GetAutomatonState() const;
    NHydra::TPeerId GetPeerId() const;
    int GetCellConfigVersion() const;
    TTabletCellConfigPtr GetCellConfig() const;
    
    NHydra::IHydraManagerPtr GetHydraManager() const;
    NRpc::IResponseKeeperPtr GetResponseKeeper() const;
    TTabletAutomatonPtr GetAutomaton() const;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    NHive::THiveManagerPtr GetHiveManager() const;
    NHive::TMailbox* GetMasterMailbox();

    TTransactionManagerPtr GetTransactionManager() const;
    NHive::TTransactionSupervisorPtr GetTransactionSupervisor() const;

    TTabletManagerPtr GetTabletManager() const;

    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);
   
    void Initialize(const NNodeTrackerClient::NProto::TCreateTabletSlotInfo& createInfo);
    void Configure(const NNodeTrackerClient::NProto::TConfigureTabletSlotInfo& configureInfo);
    void Finalize();

    void BuildOrchidYson(NYson::IYsonConsumer* consumer);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletSlot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
