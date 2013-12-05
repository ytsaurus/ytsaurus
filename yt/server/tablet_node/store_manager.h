#pragma once

#include "public.h"

#include <core/concurrency/thread_affinity.h>

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
        TTablet* tablet,
        IInvokerPtr automatonInvoker,
        IInvokerPtr compactionInvoker);

    ~TStoreManager();

    TTablet* GetTablet() const;

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

    const TDynamicMemoryStorePtr& GetActiveDynamicMemoryStore() const;

    bool IsMemoryCompactionNeeded() const;
    void RunMemoryCompaction();

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;
    IInvokerPtr AutomatonInvoker_;
    IInvokerPtr CompactionInvoker_;

    TDynamicMemoryStorePtr ActiveDynamicMemoryStore_;
    TDynamicMemoryStorePtr PassiveDynamicMemoryStore_;
    TStaticMemoryStorePtr StaticMemoryStore_;

    NVersionedTableClient::TNameTablePtr NameTable_;

    std::vector<NVersionedTableClient::TVersionedRow> PooledRowset_;

    std::unique_ptr<TMemoryCompactor> MemoryCompactor_;
    bool MemoryCompactionInProgress_;


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(CompactionThread);

    
    TDynamicRow MigrateRowIfNeeded(const TDynamicRowRef& rowRef);

    void DoMemoryCompaction();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
