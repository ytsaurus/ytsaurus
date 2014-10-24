#pragma once

#include "public.h"
#include "dynamic_memory_store_bits.h"

#include <core/logging/log.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreManager
    : public TRefCounted
{
public:
    TStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    void Initialize();

    TTablet* GetTablet() const;

    //! Returns |true| is there are outstanding locks to any of in-memory stores.
    //! Used to determine when it is safe to unmount the tablet.
    bool HasActiveLocks() const;

    //! Returns |true| is there are some in-memory stores that are not flushed yet.
    bool HasUnflushedStores() const;

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
    void SetRotationScheduled();
    void ResetRotationScheduled();
    void RotateStores(bool createNew);

    void AddStore(IStorePtr store);
    void RemoveStore(IStorePtr store);

    void CreateActiveStore();

    TDynamicMemoryStorePtr CreateDynamicMemoryStore(const TStoreId& storeId);
    TChunkStorePtr CreateChunkStore(
        NCellNode::TBootstrap* bootstrap,
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta* chunkMeta);

    bool IsStoreLocked(TDynamicMemoryStorePtr store) const;
    const yhash_set<TDynamicMemoryStorePtr>& GetLockedStores() const;

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

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

    void OnRowBlocked(
        IStore* store,
        TDynamicRow row,
        int lockIndex);
    void WaitForBlockedRow(
        IStorePtr store,
        TDynamicRow row,
        int lockIndex,
        const TTransactionId& transactionId);

};

DEFINE_REFCOUNTED_TYPE(TStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
