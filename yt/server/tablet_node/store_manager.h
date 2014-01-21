#pragma once

#include "public.h"
#include "dynamic_memory_store_bits.h"

#include <ytlib/tablet_client/public.h>

#include <ytlib/chunk_client/chunk.pb.h>

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

    bool HasActiveLocks() const;
    bool HasUnflushedStores() const;

    void LookupRow(
        TTimestamp timestamp,
        NTabletClient::TProtocolReader* reader,
        NTabletClient::TProtocolWriter* writer);
    
    void WriteRow(
        TTransaction* transaction,
        NVersionedTableClient::TUnversionedRow row,
        bool prewrite,
        std::vector<TDynamicRow>* lockedRows);

    void DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool prewrite,
        std::vector<TDynamicRow>* lockedRows);

    void ConfirmRow(const TDynamicRowRef& rowRef);
    void PrepareRow(const TDynamicRowRef& rowRef);
    void CommitRow(const TDynamicRowRef& rowRef);
    void AbortRow(const TDynamicRowRef& rowRef);

    bool IsRotationNeeded() const;
    void SetRotationScheduled();
    void ResetRotationScheduled();
    void Rotate(bool createNew);

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    bool RotationScheduled_;
    yhash_set<TDynamicMemoryStorePtr> LockedStores_;

    NVersionedTableClient::TNameTablePtr NameTable_;

    std::vector<NVersionedTableClient::TUnversionedRow> UnversionedPooledRowset_;
    std::vector<NVersionedTableClient::TVersionedRow> VersionedPooledRowset_;

    void CreateNewStore();
    
    TDynamicRow MaybeMigrateRow(const TDynamicRowRef& rowRef);

    void CheckLockAndMaybeMigrateRow(
        TTransaction* transaction,
        NVersionedTableClient::TUnversionedRow key,
        ERowLockMode mode);

    void CheckForUnlockedStore(const TDynamicMemoryStorePtr& store);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
