#pragma once

#include "public.h"
#include "dynamic_memory_store_bits.h"

#include <core/misc/chunked_memory_pool.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

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

    ~TStoreManager();

    TTablet* GetTablet() const;

    //! Returns |true| is there are outstanding locks to any of in-memory stores.
    //! Used to determine when it is safe to unmount the tablet.
    bool HasActiveLocks() const;

    //! Returns |true| is there are some in-memory stores that are not flushed yet.
    bool HasUnflushedStores() const;

    //! Executes a bunch of row lookup requests. Request parameters are parsed via #reader,
    //! response is written into #writer.
    void LookupRows(
        TTimestamp timestamp,
        NTabletClient::TWireProtocolReader* reader,
        NTabletClient::TWireProtocolWriter* writer);

    void WriteRow(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prewrite,
        std::vector<TDynamicRowRef>* lockedRowRefs);

    void DeleteRow(
        TTransaction* transaction,
        TKey key,
        bool prewrite,
        std::vector<TDynamicRowRef>* lockedRowRefs);

    void ConfirmRow(const TDynamicRowRef& rowRef);
    void PrepareRow(const TDynamicRowRef& rowRef);
    void CommitRow(const TDynamicRowRef& rowRef);
    void AbortRow(const TDynamicRowRef& rowRef);

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

    bool IsStoreLocked(TDynamicMemoryStorePtr store) const;
    const yhash_set<TDynamicMemoryStorePtr>& GetLockedStores() const;

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    bool RotationScheduled_;
    TInstant LastRotated_;

    yhash_set<TDynamicMemoryStorePtr> LockedStores_;
    yhash_set<TDynamicMemoryStorePtr> PassiveStores_;

    std::multimap<TTimestamp, IStorePtr> LatestTimestampToStore_;

    TChunkedMemoryPool LookupMemoryPool_;
    std::vector<TUnversionedRow> PooledKeys_;
    std::vector<TUnversionedRow> UnversionedPooledRows_;
    std::vector<TVersionedRow> VersionedPooledRows_;

    NLog::TTaggedLogger Logger;


    TDynamicRow MigrateRowIfNeeded(const TDynamicRowRef& rowRef);

    TDynamicMemoryStore* FindRelevantStoreAndCheckLocks(
        TTransaction* transaction,
        TUnversionedRow key,
        ERowLockMode mode,
        bool prewrite);

    void CheckForUnlockedStore(TDynamicMemoryStore* store);

    bool IsRecovery() const;

};

DEFINE_REFCOUNTED_TYPE(TStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
