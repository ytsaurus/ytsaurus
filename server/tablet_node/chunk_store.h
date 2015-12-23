#pragma once

#include "public.h"
#include "store_detail.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/data_node/public.h>

#include <yt/server/query_agent/public.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/profiling/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore
    : public TStoreBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(EStorePreloadState, PreloadState);
    DEFINE_BYVAL_RW_PROPERTY(TFuture<void>, PreloadFuture);

    DEFINE_BYVAL_RW_PROPERTY(EStoreCompactionState, CompactionState);

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

    EInMemoryMode GetInMemoryMode() const;
    void SetInMemoryMode(EInMemoryMode mode);
    void Preload(TInMemoryChunkDataPtr chunkData);

    NChunkClient::IChunkReaderPtr GetChunkReader();

    // IStore implementation.
    virtual EStoreType GetType() const override;

    virtual i64 GetUncompressedDataSize() const override;
    virtual i64 GetRowCount() const override;

    virtual TOwningKey GetMinKey() const override;
    virtual TOwningKey GetMaxKey() const override;

    virtual TTimestamp GetMinTimestamp() const override;
    virtual TTimestamp GetMaxTimestamp() const override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual void CheckRowLocks(
        TUnversionedRow row,
        TTransaction* transaction,
        ui32 lockMask) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual TCallback<void(TSaveContext&)> AsyncSave() override;
    virtual void AsyncLoad(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

private:
    class TPreloadedBlockCache;
    using TPreloadedBlockCachePtr = TIntrusivePtr<TPreloadedBlockCache>;

    NCellNode::TBootstrap* const Bootstrap_;

    // Cached for fast retrieval from ChunkMeta_.
    TOwningKey MinKey_;
    TOwningKey MaxKey_;
    TTimestamp MinTimestamp_;
    TTimestamp MaxTimestamp_;
    i64 DataSize_ = -1;
    i64 RowCount_ = -1;

    NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_;

    NConcurrency::TReaderWriterSpinLock SpinLock_;

    bool ChunkInitialized_ = false;
    NDataNode::IChunkPtr Chunk_;

    NChunkClient::IChunkReaderPtr ChunkReader_;

    NTableClient::TCachedVersionedChunkMetaPtr CachedVersionedChunkMeta_;

    IStorePtr BackingStore_;

    TPreloadedBlockCachePtr PreloadedBlockCache_;

    EInMemoryMode InMemoryMode_ = EInMemoryMode::None;

    const NTableClient::TKeyComparer KeyComparer_;

    NTableClient::IVersionedReaderPtr CreateCacheBasedReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter);
    NTableClient::IVersionedReaderPtr CreateCacheBasedReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter);

    NDataNode::IChunkPtr PrepareChunk();
    NChunkClient::IChunkReaderPtr PrepareChunkReader(
        NDataNode::IChunkPtr chunk);
    NTableClient::TCachedVersionedChunkMetaPtr PrepareCachedVersionedChunkMeta(
        NChunkClient::IChunkReaderPtr chunkReader);
    IStorePtr GetBackingStore();
    NChunkClient::IBlockCachePtr GetBlockCache();

    void PrecacheProperties();

    void OnLocalReaderFailed();
    void OnChunkExpired();
    void OnChunkReaderExpired();

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
