#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Tablet write manager is closely linked to the tablet manager which acts as a host
//! for tablet write manager. The following interface specifies methods of tablet manager
//! required by the tablet write manager and provides means for unit-testing of tablet write manager.
struct ITabletWriteManagerHost
    : public virtual TRefCounted
{
    virtual TCellId GetCellId() const = 0;

    //! This method is called whenever a (sorted) tablet row is being unlocked.
    virtual void OnTabletRowUnlocked(TTablet* tablet) = 0;
    //! This method is called whenever tablet lock count decreases.
    virtual void OnTabletUnlocked(TTablet* tablet) = 0;

    virtual TTablet* GetTabletOrThrow(TTabletId id) = 0;
    virtual TTablet* FindTablet(const TTabletId& id) const = 0;
    virtual const NHydra::TReadOnlyEntityMap<TTablet>& Tablets() const = 0;

    virtual TTransactionManagerPtr GetTransactionManager() const = 0;
    virtual NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() const = 0;
    virtual TTabletManagerConfigPtr GetConfig() const = 0;

    virtual void ValidateMemoryLimit(const std::optional<TString>& poolTag) = 0;
    virtual NTransactionClient::TTimestamp GetLatestTimestamp() const = 0;

    virtual bool ValidateAndDiscardRowRef(const TSortedDynamicRowRef& rowRef) = 0;

    virtual void AdvanceReplicatedTrimmedRowCount(TTablet* tablet, TTransaction* transaction) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletWriteManagerHost);

////////////////////////////////////////////////////////////////////////////////

//! A component containing tablet write logic: dynamic store writing,
//! row prelocking/locking, 1pc/2pc details.
struct ITabletWriteManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void Write(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTransactionId transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        TTransactionGeneration generation,
        int rowCount,
        size_t dataWeight,
        bool versioned,
        const TSyncReplicaIdList& syncReplicaIds,
        NTableClient::TWireProtocolReader* reader,
        TFuture<void>* commitResult) = 0;

    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletWriteManager);

////////////////////////////////////////////////////////////////////////////////

ITabletWriteManagerPtr CreateTabletWriteManager(
    ITabletWriteManagerHostPtr host,
    NHydra::ISimpleHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    TMemoryUsageTrackerGuard&& writeLogsMemoryTrackerGuard,
    IInvokerPtr automatonInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
