#pragma once

#include "public.h"
#include "dynamic_memory_store_bits.h"

#include <core/logging/log.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages a set of stores comprising a tablet.
/*!
 *  This class provides a facade for modifying data within a given tablet.
 *
 *  Each tablet has an instance of TStoreManager, which is attached to the tablet
 *  upon its construction.
 *
 *  TStoreManager instances are not bound to any specific epoch and are reused.
 */
class TStoreManager
    : public TRefCounted
{
public:
    TStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        TCallback<TDynamicMemoryStorePtr()> dynamicMemoryStoreFactory);

    //! Returns the tablet this instance is bound to.
    TTablet* GetTablet() const;

    //! Returns |true| if there are outstanding locks to any of dynamic memory stores.
    //! Used to determine when it is safe to unmount the tablet.
    bool HasActiveLocks() const;

    //! Returns |true| if there are some dynamic memory stores that are not flushed yet.
    bool HasUnflushedStores() const;

    void StartEpoch(TTabletSlotPtr slot);
    void StopEpoch();

    TDynamicRowRef WriteRowAtomic(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock);

    void WriteRowNonAtomic(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp,
        TUnversionedRow row);

    TDynamicRowRef DeleteRowAtomic(
        TTransaction* transaction,
        TKey key,
        bool prelock);

    void DeleteRowNonAtomic(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp,
        TKey key);

    void ConfirmRow(TTransaction* transaction, const TDynamicRowRef& rowRef);
    void PrepareRow(TTransaction* transaction, const TDynamicRowRef& rowRef);
    void CommitRow(TTransaction* transaction, const TDynamicRowRef& rowRef);
    void AbortRow(TTransaction* transaction, const TDynamicRowRef& rowRef);

    bool IsOverflowRotationNeeded() const;
    bool IsPeriodicRotationNeeded() const;
    bool IsRotationPossible() const;
    bool IsForcedRotationPossible() const;
    bool IsRotationScheduled() const;
    void ScheduleRotation();
    void Rotate(bool createNewStore);

    void AddStore(IStorePtr store);
    void RemoveStore(IStorePtr store);
    void CreateActiveStore();

    bool IsStoreLocked(TDynamicMemoryStorePtr store) const;
    const yhash_set<TDynamicMemoryStorePtr>& GetLockedStores() const;

    static bool IsStoreFlushable(IStorePtr store);
    void BeginStoreFlush(TDynamicMemoryStorePtr store);
    void EndStoreFlush(TDynamicMemoryStorePtr store);
    void BackoffStoreFlush(TDynamicMemoryStorePtr store);

    static bool IsStoreCompactable(IStorePtr store);
    void BeginStoreCompaction(TChunkStorePtr store);
    void EndStoreCompaction(TChunkStorePtr store);
    void BackoffStoreCompaction(TChunkStorePtr store);

    void ScheduleStorePreload(TChunkStorePtr store);
    bool TryPreloadStoreFromInterceptedData(TChunkStorePtr store, TInterceptedChunkDataPtr chunkData);
    TChunkStorePtr PeekStoreForPreload();
    void BeginStorePreload(TChunkStorePtr store, TFuture<void> future);
    void EndStorePreload(TChunkStorePtr store);
    void BackoffStorePreload(TChunkStorePtr store);

    void Remount(
        TTableMountConfigPtr mountConfig,
        TTabletWriterOptionsPtr writerOptions);

private:
    const TTabletManagerConfigPtr Config_;
    TTablet* const Tablet_;
    const TCallback<TDynamicMemoryStorePtr()> DynamicMemoryStoreFactory_;

    int KeyColumnCount_;

    bool RotationScheduled_;
    TInstant LastRotated_;

    yhash_set<TDynamicMemoryStorePtr> LockedStores_;
    std::multimap<TTimestamp, IStorePtr> MaxTimestampToStore_;

    NLogging::TLogger Logger;


    ui32 ComputeLockMask(TUnversionedRow row);

    void CheckInactiveStoresLocks(
        TTransaction* transaction,
        TUnversionedRow row,
        ui32 lockMask);

    void CheckForUnlockedStore(TDynamicMemoryStore* store);

    bool IsRecovery() const;

    void UpdateInMemoryMode();

};

DEFINE_REFCOUNTED_TYPE(TStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
