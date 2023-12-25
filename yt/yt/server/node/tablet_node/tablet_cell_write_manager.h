#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Tablet write manager is closely linked to the tablet manager which acts as a host
//! for tablet write manager. The following interface specifies methods of tablet manager
//! required by the tablet write manager and provides means for unit-testing of tablet write manager.
struct ITabletCellWriteManagerHost
    : public virtual TRefCounted
{
    virtual TTabletNodeDynamicConfigPtr GetDynamicConfig() const = 0;

    virtual TCellId GetCellId() const = 0;

    virtual const NLeaseServer::ILeaseManagerPtr& GetLeaseManager() const = 0;
    virtual TFuture<void> IssueLeases(const std::vector<NLeaseServer::TLeaseId>& leaseIds) = 0;

    //! This method is called whenever a (sorted) tablet row is being unlocked.
    virtual void OnTabletRowUnlocked(TTablet* tablet) = 0;

    virtual i64 LockTablet(TTablet* tablet, ETabletLockType lockType) = 0;
    virtual i64 UnlockTablet(TTablet* tablet, ETabletLockType lockType) = 0;

    virtual TTablet* GetTabletOrThrow(TTabletId id) = 0;
    virtual TTablet* FindTablet(const TTabletId& id) const = 0;
    virtual TTablet* GetTablet(const TTabletId& id) const = 0;
    virtual const NHydra::TReadOnlyEntityMap<TTablet>& Tablets() const = 0;

    virtual ITransactionManagerPtr GetTransactionManager() const = 0;
    virtual NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() const = 0;
    virtual TTabletManagerConfigPtr GetConfig() const = 0;

    virtual void ValidateMemoryLimit(const std::optional<TString>& poolTag) = 0;
    virtual NTransactionClient::TTimestamp GetLatestTimestamp() const = 0;

    virtual bool ValidateRowRef(const TSortedDynamicRowRef& rowRef) = 0;
    virtual bool ValidateAndDiscardRowRef(const TSortedDynamicRowRef& rowRef) = 0;

    virtual void AdvanceReplicatedTrimmedRowCount(TTablet* tablet, TTransaction* transaction) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletCellWriteManagerHost)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellWriteParams
{
    TTransactionId TransactionId;
    TTimestamp TransactionStartTimestamp;
    TDuration TransactionTimeout;
    TTransactionSignature PrepareSignature = NTransactionClient::InitialTransactionSignature;
    TTransactionSignature CommitSignature = NTransactionClient::InitialTransactionSignature;
    TTransactionGeneration Generation = NTransactionClient::InitialTransactionGeneration;
    int RowCount = 0;
    i64 DataWeight = 0;
    bool Versioned = false;
    TSyncReplicaIdList SyncReplicaIds;

    std::optional<NTableClient::THunkChunksInfo> HunkChunksInfo;

    std::vector<TTransactionId> PrerequisiteTransactionIds;
};

//! A component containing tablet write logic: dynamic store writing,
//! row prelocking/locking, 1PC/2PC details.
struct ITabletCellWriteManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual TFuture<void> Write(
        const TTabletSnapshotPtr& tabletSnapshot,
        NTableClient::IWireProtocolReader* reader,
        const TTabletCellWriteParams& params) = 0;

    // Tablet locking stuff.
    virtual void AddTransientAffectedTablet(TTransaction* transaction, TTablet* tablet) = 0;
    virtual void AddPersistentAffectedTablet(TTransaction* transaction, TTablet* tablet) = 0;

    DECLARE_INTERFACE_SIGNAL(void(TTablet*), ReplicatorWriteTransactionFinished);
};

DEFINE_REFCOUNTED_TYPE(ITabletCellWriteManager)

////////////////////////////////////////////////////////////////////////////////

ITabletCellWriteManagerPtr CreateTabletCellWriteManager(
    ITabletCellWriteManagerHostPtr host,
    NHydra::ISimpleHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    IMutationForwarderPtr mutationForwarder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
