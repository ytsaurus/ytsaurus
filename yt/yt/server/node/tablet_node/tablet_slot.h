#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

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
struct ITabletSlot
    : public NCellarAgent::ICellarOccupier
{
    static constexpr auto CellarType = NCellarClient::ECellarType::Tablet;

    virtual NHydra::TCellId GetCellId() = 0;
    virtual NHydra::EPeerState GetAutomatonState() = 0;

    virtual const TString& GetTabletCellBundleName() = 0;

    virtual NHydra::IDistributedHydraManagerPtr GetHydraManager() = 0;

    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() = 0;
    virtual const TTransactionManagerPtr& GetTransactionManager() = 0;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;

    virtual const NHiveServer::THiveManagerPtr& GetHiveManager() = 0;
    virtual NHiveServer::TMailbox* GetMasterMailbox() = 0;

    virtual const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() = 0;

    virtual const TTabletManagerPtr& GetTabletManager() = 0;

    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) = 0;

    virtual const TRuntimeTabletCellDataPtr& GetRuntimeData() = 0;

    virtual double GetUsedCpu(double cpuPerTabletSlot) = 0;

    virtual NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() = 0;
    virtual NTabletClient::TTabletCellOptionsPtr GetOptions() = 0;

    virtual NChunkClient::IChunkFragmentReaderPtr CreateChunkFragmentReader(TTablet* tablet) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletSlot)

ITabletSlotPtr CreateTabletSlot(
    int slotIndex,
    TTabletNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
