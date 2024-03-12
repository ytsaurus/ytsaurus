#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/tablet_node/tablet_memory_statistics.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/server/lib/security_server/public.h>

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
    virtual int GetAutomatonTerm() = 0;

    virtual const TString& GetTabletCellBundleName() = 0;

    virtual NHydra::IDistributedHydraManagerPtr GetHydraManager() = 0;
    virtual NHydra::ISimpleHydraManagerPtr GetSimpleHydraManager() = 0;

    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() = 0;

    virtual const ITransactionManagerPtr& GetTransactionManager() = 0;

    virtual const IDistributedThrottlerManagerPtr& GetDistributedThrottlerManager() = 0;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;

    virtual const IMutationForwarderPtr& GetMutationForwarder() = 0;

    virtual const NHiveServer::IHiveManagerPtr& GetHiveManager() = 0;
    virtual const NHiveServer::TSimpleAvenueDirectoryPtr& GetAvenueDirectory() = 0;
    virtual NHiveServer::TMailbox* GetMasterMailbox() = 0;

    virtual void RegisterMasterAvenue(
        TTabletId tabletId,
        NHiveServer::TAvenueEndpointId masterEndpointId,
        NHiveServer::TPersistentMailboxState&& cookie) = 0;

    virtual NHiveServer::TPersistentMailboxState UnregisterMasterAvenue(
        NHiveServer::TAvenueEndpointId masterEndpointId) = 0;

    virtual void RegisterSiblingTabletAvenue(
        NHiveServer::TAvenueEndpointId siblingEndpointId,
        TCellId siblingCellId) = 0;

    virtual void UnregisterSiblingTabletAvenue(
        NHiveServer::TAvenueEndpointId siblingEndpointId) = 0;

    virtual void CommitTabletMutation(const ::google::protobuf::MessageLite& message) = 0;
    virtual void PostMasterMessage(TTabletId tabletId, const ::google::protobuf::MessageLite& message) = 0;

    virtual const NTransactionSupervisor::ITransactionSupervisorPtr& GetTransactionSupervisor() = 0;

    virtual const NLeaseServer::ILeaseManagerPtr& GetLeaseManager() = 0;

    virtual ITabletManagerPtr GetTabletManager() = 0;
    virtual const ITabletCellWriteManagerPtr& GetTabletCellWriteManager() = 0;
    virtual const ISmoothMovementTrackerPtr& GetSmoothMovementTracker() = 0;

    virtual const IHunkTabletManagerPtr& GetHunkTabletManager() = 0;

    virtual const NSecurityServer::IResourceLimitsManagerPtr& GetResourceLimitsManager() const = 0;

    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) = 0;

    virtual const TRuntimeTabletCellDataPtr& GetRuntimeData() = 0;

    virtual double GetUsedCpu(double cpuPerTabletSlot) = 0;

    virtual NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() = 0;
    virtual NTabletClient::TTabletCellOptionsPtr GetOptions() const = 0;

    virtual NChunkClient::IChunkFragmentReaderPtr CreateChunkFragmentReader(TTablet* tablet) = 0;
    virtual ICompressionDictionaryManagerPtr GetCompressionDictionaryManager() const = 0;

    virtual NTransactionClient::TTimestamp GetLatestTimestamp() = 0;
    virtual NObjectClient::TCellTag GetNativeCellTag() = 0;

    virtual TFuture<TTabletCellMemoryStatistics> GetMemoryStatistics() = 0;

    virtual bool IsTabletEpochActive() const = 0;

    virtual int EstimateChangelogMediumBytes(int payloadBytes) const = 0;
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetChangelogMediumWriteThrottler() const = 0;
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetMediumWriteThrottler(const TString& mediumName) const = 0;
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetMediumReadThrottler(const TString& mediumName) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletSlot)

////////////////////////////////////////////////////////////////////////////////

ITabletSlotPtr CreateTabletSlot(
    int slotIndex,
    TTabletNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
