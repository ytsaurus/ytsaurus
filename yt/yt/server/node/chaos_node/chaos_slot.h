#pragma once

#include "public.h"
#include "bootstrap.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/chaos_node/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/server/lib/tablet_server/public.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IChaosAutomatonHost
    : public virtual TRefCounted
{
    virtual NHydra::TCellId GetCellId() const = 0;
    virtual const IInvokerPtr& GetAsyncSnapshotInvoker() const = 0;
    virtual const NLeaseServer::ILeaseManagerPtr& GetLeaseManager() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosAutomatonHost);

////////////////////////////////////////////////////////////////////////////////

//! An instance of Chaos Hydra.
struct IChaosSlot
    : public NCellarAgent::ICellarOccupier
    , public IChaosAutomatonHost
{
    static constexpr NCellarClient::ECellarType CellarType = NCellarClient::ECellarType::Chaos;

    virtual const TString& GetCellBundleName() const = 0;
    virtual NHydra::EPeerState GetAutomatonState() const = 0;

    virtual NHydra::IDistributedHydraManagerPtr GetHydraManager() const = 0;
    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() const = 0;
    virtual ITransactionManagerPtr GetTransactionManager() const = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const = 0;
    virtual const NTransactionSupervisor::ITransactionSupervisorPtr& GetTransactionSupervisor() const = 0;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const = 0;

    virtual const NHiveServer::IHiveManagerPtr& GetHiveManager() const = 0;
    virtual NHiveServer::TMailboxHandle GetMasterMailbox() = 0;

    virtual const IChaosManagerPtr& GetChaosManager() const = 0;
    virtual const NChaosClient::IReplicationCardsWatcherPtr& GetReplicationCardsWatcher() const = 0;
    virtual const ICoordinatorManagerPtr& GetCoordinatorManager() const = 0;
    virtual const IShortcutSnapshotStorePtr& GetShortcutSnapshotStore() const = 0;

    virtual const IInvokerPtr& GetSnapshotStoreReadPoolInvoker() const = 0;

    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) = 0;

    virtual NApi::IClientPtr CreateClusterClient(const TString& clusterName) const = 0;
    virtual const NTabletServer::IReplicatedTableTrackerPtr& GetReplicatedTableTracker() const = 0;
    virtual NTabletServer::TDynamicReplicatedTableTrackerConfigPtr GetReplicatedTableTrackerConfig() const = 0;
    virtual bool IsExtendedLoggingEnabled() const = 0;
    virtual void Reconfigure(const TChaosNodeDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosSlot)

IChaosSlotPtr CreateChaosSlot(
    int slotIndex,
    TChaosNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
