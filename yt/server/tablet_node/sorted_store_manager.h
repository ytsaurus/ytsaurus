#pragma once

#include "store_manager_detail.h"
#include "sorted_dynamic_store_bits.h"
#include "in_memory_manager.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreManager
    : public TStoreManagerBase
{
public:
    TSortedStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        ITabletContext* tabletContext,
        NHydra::IHydraManagerPtr hydraManager,
        TInMemoryManagerPtr inMemoryManager);

    virtual bool HasActiveLocks() const override;

    virtual void ExecuteAtomicWrite(
        TTablet* tablet,
        TTransaction* transaction,
        NTabletClient::TWireProtocolReader* reader,
        bool prelock) override;
    virtual void ExecuteNonAtomicWrite(
        TTablet* tablet,
        NTransactionClient::TTimestamp commitTimestamp,
        NTabletClient::TWireProtocolReader* reader) override;

    TSortedDynamicRowRef WriteRowAtomic(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock);
    void WriteRowNonAtomic(
        TTimestamp commitTimestamp,
        TUnversionedRow row);
    TSortedDynamicRowRef DeleteRowAtomic(
        TTransaction* transaction,
        TKey key,
        bool prelock);
    void DeleteRowNonAtomic(
        TTimestamp commitTimestamp,
        TKey key);

    static void LockRow(TTransaction* transaction, bool prelock, const TSortedDynamicRowRef& rowRef);
    void ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);
    void PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);
    void CommitRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);
    void AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);

    virtual bool IsOverflowRotationNeeded() const override;
    virtual bool IsPeriodicRotationNeeded() const override;
    virtual bool IsRotationPossible() const override;
    virtual bool IsForcedRotationPossible() const override;
    virtual void Rotate(bool createNewStore) override;

    virtual void AddStore(IStorePtr store, bool onMount) override;
    virtual void RemoveStore(IStorePtr store) override;

    virtual void CreateActiveStore() override;

    virtual bool IsStoreLocked(IStorePtr store) const override;
    virtual std::vector<IStorePtr> GetLockedStores() const override;

    virtual bool IsStoreCompactable(IStorePtr store) const override;

private:
    const int KeyColumnCount_;

    TSortedDynamicStorePtr ActiveStore_;
    yhash_set<TSortedDynamicStorePtr> LockedStores_;
    std::multimap<TTimestamp, ISortedStorePtr> MaxTimestampToStore_;


    ui32 ComputeLockMask(TUnversionedRow row);

    void CheckInactiveStoresLocks(
        TTransaction* transaction,
        TUnversionedRow row,
        ui32 lockMask);

    void CheckForUnlockedStore(TSortedDynamicStore* store);

    void ValidateActiveStoreOverflow();
    void ValidateOnWrite(const TTransactionId& transactionId, TUnversionedRow row);
    void ValidateOnDelete(const TTransactionId& transactionId, TKey key);

};

DEFINE_REFCOUNTED_TYPE(TSortedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
