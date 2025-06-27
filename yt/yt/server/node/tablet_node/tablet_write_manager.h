#pragma once

#include "public.h"

#include "dynamic_store_bits.h"
#include "serialize.h"
#include "transaction.h"
#include "write_log.h"

#include <yt/yt/core/concurrency/async_barrier.h>
#include <yt/yt/core/misc/persistent_queue.h>

#include <library/cpp/containers/bitset/bitset.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct ITabletWriteManagerHost
    : public virtual TRefCounted
{
    //! This method is called whenever a (sorted) tablet row is being unlocked.
    virtual void OnTabletRowUnlocked(TTablet* tablet) = 0;

    virtual ITransactionManagerPtr GetTransactionManager() const = 0;
    virtual TTabletManagerConfigPtr GetConfig() const = 0;

    virtual NHydra::ISimpleHydraManagerPtr GetHydraManager() const = 0;

    virtual void AdvanceReplicatedTrimmedRowCount(TTablet* tablet, TTransaction* transaction) = 0;

    virtual bool ValidateRowRef(const TSortedDynamicRowRef& rowRef) = 0;
    virtual bool ValidateAndDiscardRowRef(const TSortedDynamicRowRef& rowRef) = 0;

    virtual const IBackupManagerPtr& GetBackupManager() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletWriteManagerHost)

////////////////////////////////////////////////////////////////////////////////

struct ITabletWriteManager
    : public TRefCounted
{
    virtual TWriteContext TransientWriteRows(
        TTransaction* transaction,
        IWireWriteCommandsReader* reader,
        NTransactionClient::EAtomicity atomicity,
        bool versioned,
        int rowCount,
        i64 dataWeight) = 0;

    virtual void AtomicLeaderWriteRows(
        TTransaction* transaction,
        TTransactionGeneration generation,
        const TTransactionWriteRecord& record,
        bool lockless) = 0;

    virtual void AtomicFollowerWriteRows(
        TTransaction* transaction,
        const TTransactionWriteRecord& record,
        bool lockless) = 0;

    virtual void NonAtomicWriteRows(
        TTransactionId transactionId,
        const TTransactionWriteRecord& record,
        bool isLeader) = 0;

    virtual void WriteDelayedRows(
        TTransaction* transaction,
        const TTransactionWriteRecord& record,
        bool lockless) = 0;

    virtual void OnTransactionPrepared(TTransaction* transaction, bool persistent) = 0;
    virtual void OnTransactionCommitted(TTransaction* transaction) = 0;
    virtual void OnTransactionAborted(TTransaction* transaction) = 0;
    virtual void OnTransactionCoarselySerialized(TTransaction* transaction) = 0;
    virtual void OnTransactionPerRowSerialized(TTransaction* transaction) = 0;

    // Interface for the store manager to process part.
    virtual void OnTransactionPartCommitted(
        TTransaction* transaction,
        const TSortedDynamicRowRef& rowRef,
        int lockIndex,
        TOpaqueWriteLogIndex writeLogIndex,
        bool onAfterSnapshotLoaded) = 0;

    virtual void OnTransactionTransientReset(TTransaction* transaction) = 0;

    virtual void OnTransientGenerationPromoted(TTransaction* transaction) = 0;
    virtual void OnPersistentGenerationPromoted(TTransaction* transaction) = 0;

    virtual bool NeedsSerialization(TTransaction* transaction) = 0;

    virtual void UpdateReplicationProgress(TTransaction* transaction) = 0;

    virtual void BuildOrchidYson(TTransaction* transaction, NYson::IYsonConsumer* consumer) = 0;

    virtual bool HasUnfinishedTransientTransactions() const = 0;
    virtual bool HasUnfinishedPersistentTransactions() const = 0;

    //! Returns all transactions that affect the tablet, both transient and persistent.
    virtual THashSet<TTransactionId> GetAffectingTransactionIds() const = 0;

    virtual void StartEpoch() = 0;
    virtual void StopEpoch() = 0;

    virtual void Clear() = 0;

    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;

    virtual TCallback<void(TSaveContext&)> AsyncSave() = 0;
    virtual void AsyncLoad(TLoadContext& context) = 0;

    virtual void OnAfterSnapshotLoaded() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletWriteManager)

////////////////////////////////////////////////////////////////////////////////

ITabletWriteManagerPtr CreateTabletWriteManager(
    TTablet* tablet,
    ITabletContext* tabletContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
