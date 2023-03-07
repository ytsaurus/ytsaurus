#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! All fields must be atomic since they're being accessed both
//! from the writer and from readers concurrently.
struct TRuntimeTabletCellData
    : public TIntrinsicRefCounted
{
    std::atomic<TTimestamp> BarrierTimestamp = {MinTimestamp};
};

DEFINE_REFCOUNTED_TYPE(TRuntimeTabletCellData)

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra managing a number of tablets.
class TTabletSlot
    : public TRefCounted
{
public:
    TTabletSlot(
        int slotIndex,
        TTabletNodeConfigPtr config,
        const NTabletClient::NProto::TCreateTabletSlotInfo& createInfo,
        NClusterNode::TBootstrap* bootstrap);

    ~TTabletSlot();

    int GetIndex() const;
    NHydra::TCellId GetCellId() const;
    NHydra::EPeerState GetControlState() const;
    NHydra::EPeerState GetAutomatonState() const;
    NHydra::TPeerId GetPeerId() const;
    const NHiveClient::TCellDescriptor& GetCellDescriptor() const;

    const NHydra::IDistributedHydraManagerPtr& GetHydraManager() const;
    const NRpc::TResponseKeeperPtr& GetResponseKeeper() const;
    const TTabletAutomatonPtr& GetAutomaton() const;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    const NHiveServer::THiveManagerPtr& GetHiveManager() const;
    NHiveServer::TMailbox* GetMasterMailbox();

    const TTransactionManagerPtr& GetTransactionManager() const;
    const NHiveServer::TTransactionSupervisorPtr& GetTransactionSupervisor() const;

    const TTabletManagerPtr& GetTabletManager() const;

    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);

    void Initialize();
    bool CanConfigure() const;
    void Configure(const NTabletClient::NProto::TConfigureTabletSlotInfo& configureInfo);
    TFuture<void> Finalize();

    const NYTree::IYPathServicePtr& GetOrchidService();

    const NProfiling::TTagIdList& GetProfilingTagIds();

    const TRuntimeTabletCellDataPtr& GetRuntimeData() const;

    double GetUsedCpu(double cpuPerTabletSlot) const;

    NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() const;
    NTabletClient::TTabletCellOptionsPtr GetOptions() const;

    i32 GetDynamicConfigVersion() const;
    void UpdateDynamicConfig(const NTabletClient::NProto::TUpdateTabletSlotInfo& updateInfo);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletSlot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
