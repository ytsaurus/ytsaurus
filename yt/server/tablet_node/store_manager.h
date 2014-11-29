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

    TDynamicRowRef WriteRow(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock,
        ELockMode lockMode = ELockMode::Row);

    TDynamicRowRef DeleteRow(
        TTransaction* transaction,
        TKey key,
        bool prelock);

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

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;
    TCallback<TDynamicMemoryStorePtr()> DynamicMemoryStoreFactory_;

    int KeyColumnCount_;

    bool RotationScheduled_;
    TInstant LastRotated_;

    yhash_set<TDynamicMemoryStorePtr> LockedStores_;
    std::multimap<TTimestamp, IStorePtr> MaxTimestampToStore_;

    NLog::TLogger Logger;


    ui32 ComputeLockMask(TUnversionedRow row, ELockMode lockMode);

    void CheckInactiveStoresLocks(
        TTransaction* transaction,
        TUnversionedRow key,
        ui32 lockMask);

    void CheckForUnlockedStore(TDynamicMemoryStore* store);

    bool IsRecovery() const;

};

DEFINE_REFCOUNTED_TYPE(TStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
