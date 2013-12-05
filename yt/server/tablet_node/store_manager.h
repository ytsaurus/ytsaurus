#pragma once

#include "public.h"

#include <core/concurrency/thread_affinity.h>

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TColumnFilter
{
    TColumnFilter()
        : All(true)
    { }

    TColumnFilter(const std::vector<Stroka>& columns)
        : All(false)
        , Columns(columns.begin(), columns.end())
    { }

    TColumnFilter(const TColumnFilter& other)
        : All(other.All)
        , Columns(other.Columns)
    { }

    bool All;
    TSmallVector<Stroka, NVersionedTableClient::TypicalColumnCount> Columns;
};

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

    TTablet* GetTablet() const;

    void Lookup(
        NVersionedTableClient::TKey key,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        NChunkClient::NProto::TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks);
    
    void Write(
        TTransaction* transaction,
        NChunkClient::NProto::TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks,
        bool prewrite,
        std::vector<TDynamicRow>* lockedRows);

    void Delete(
        TTransaction* transaction,
        const std::vector<NVersionedTableClient::TOwningKey>& keys,
        bool predelete,
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

    std::unique_ptr<TMemoryCompactor> MemoryCompactor_;
    bool MemoryCompactionInProgress_;


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(CompactionThread);


    void DoMemoryCompaction();
    void FinishMemoryCompaction(TStaticMemoryStorePtr compactedStore);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
