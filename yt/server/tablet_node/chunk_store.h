#pragma once

#include "public.h"
#include "store_detail.h"

#include <core/misc/nullable.h>

#include <core/concurrency/rw_spinlock.h>

#include <core/rpc/public.h>

#include <core/profiling/public.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_row.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/cell_node/public.h>

#include <server/query_agent/public.h>

#include <server/data_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore
    : public TStoreBase
{
public:
    TChunkStore(
        const TStoreId& id,
        TTablet* tablet,
        const NChunkClient::NProto::TChunkMeta* chunkMeta,
        NCellNode::TBootstrap* bootstrap);
    ~TChunkStore();

    const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const;

    void SetBackingStore(IStorePtr store);
    bool HasBackingStore() const;

    // IStore implementation.
    virtual EStoreType GetType() const override;

    virtual i64 GetUncompressedDataSize() const override;
    virtual i64 GetRowCount() const override;

    virtual TOwningKey GetMinKey() const override;
    virtual TOwningKey GetMaxKey() const override;

    virtual TTimestamp GetMinTimestamp() const override;
    virtual TTimestamp GetMaxTimestamp() const override;

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual NVersionedTableClient::IVersionedLookuperPtr CreateLookuper(
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual void CheckRowLocks(
        TKey key,
        TTransaction* transaction,
        ui32 lockMask) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

private:
    class TLocalChunkReaderWrapper;
    class TVersionedReaderWrapper;
    class TVersionedLookuperWrapper;

    NCellNode::TBootstrap* const Bootstrap_;

    // Cached for fast retrieval from ChunkMeta_.
    TOwningKey MinKey_;
    TOwningKey MaxKey_;
    TTimestamp MinTimestamp_;
    TTimestamp MaxTimestamp_;
    i64 DataSize_ = -1;
    i64 RowCount_ = -1;

    NChunkClient::NProto::TChunkMeta ChunkMeta_;

    NConcurrency::TReaderWriterSpinLock ChunkLock_;
    bool ChunkInitialized_ = false;
    NDataNode::IChunkPtr Chunk_;

    NConcurrency::TReaderWriterSpinLock ChunkReaderLock_;
    NChunkClient::IChunkReaderPtr ChunkReader_;

    NConcurrency::TReaderWriterSpinLock CachedVersionedChunkMetaLock_;
    NVersionedTableClient::TCachedVersionedChunkMetaPtr CachedVersionedChunkMeta_;

    NConcurrency::TReaderWriterSpinLock BackingStoreLock_;
    IStorePtr BackingStore_;


    NDataNode::IChunkPtr PrepareChunk();
    NDataNode::IChunkPtr DoFindChunk();
    NChunkClient::IChunkReaderPtr PrepareChunkReader(
        NDataNode::IChunkPtr chunk);
    NVersionedTableClient::TCachedVersionedChunkMetaPtr PrepareCachedVersionedChunkMeta(
        NChunkClient::IChunkReaderPtr chunkReader);
    IStorePtr GetBackingStore();

    void PrecacheProperties();

    void OnLocalReaderFailed();

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
