#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! All fields must be atomic since they're being accessed both
//! from the writer and from readers concurrently.
struct TRuntimeTabletCellData
    : public TRefCounted
{
    std::atomic<TTimestamp> BarrierTimestamp = {MinTimestamp};
};

DEFINE_REFCOUNTED_TYPE(TRuntimeTabletCellData)

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra managing a number of tablets.
class TTabletSlot
    : public NCellarAgent::ICellarOccupier
{
public:
    static constexpr NCellarAgent::ECellarType CellarType = NCellarAgent::ECellarType::Tablet;

public:
    TTabletSlot(
        int slotIndex,
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);
    ~TTabletSlot();

    virtual void SetOccupant(NCellarAgent::ICellarOccupantPtr occupant) override;
    virtual NHydra::TCompositeAutomatonPtr CreateAutomaton() override;
    virtual void Configure(NHydra::IDistributedHydraManagerPtr hydraManager) override;
    virtual const NHiveServer::ITransactionManagerPtr GetOccupierTransactionManager() override;
    virtual void Initialize() override;
    virtual void RegisterRpcServices() override;
    virtual IInvokerPtr GetOccupierAutomatonInvoker() override;
    virtual IInvokerPtr GetMutationAutomatonInvoker() override;
    virtual NYTree::TCompositeMapServicePtr PopulateOrchidService(
        NYTree::TCompositeMapServicePtr orchidService) override;
    virtual void Stop() override;
    virtual void Finalize() override;
    virtual NCellarAgent::ECellarType GetCellarType() override;
    virtual NProfiling::TProfiler GetProfiler() override;
    
    NHydra::TCellId GetCellId() const;
    NHydra::EPeerState GetAutomatonState() const;

    const TString& GetTabletCellBundleName() const;

    NHydra::IDistributedHydraManagerPtr GetHydraManager() const;
    
    const NHydra::TCompositeAutomatonPtr& GetAutomaton() const;
    const TTransactionManagerPtr& GetTransactionManager() const;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    const NHiveServer::THiveManagerPtr& GetHiveManager() const;
    NHiveServer::TMailbox* GetMasterMailbox();

    const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() const;

    const TTabletManagerPtr& GetTabletManager() const;

    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);

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
