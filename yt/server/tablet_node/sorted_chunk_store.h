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

class TSortedChunkStore
    : public TChunkStoreBase
    , public TSortedStoreBase
{
public:
    TSortedChunkStore(
        const TStoreId& id,
        TTablet* tablet,
        const NChunkClient::NProto::TChunkMeta* chunkMeta,
        NCellNode::TBootstrap* bootstrap);
    ~TSortedChunkStore();

    const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const;

    void SetBackingStore(ISortedStorePtr store);
    bool HasBackingStore() const;

    // IChunkStore implementation.
    virtual EInMemoryMode GetInMemoryMode() const override;
    virtual void SetInMemoryMode(EInMemoryMode mode) override;

    virtual NChunkClient::IChunkReaderPtr GetChunkReader() override;

    virtual void Preload(TInMemoryChunkDataPtr chunkData) override;

    // ISortedStore implementation.
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
        const TColumnFilter& columnFilter,
        const TWorkloadDescriptor& workloadDescriptor) override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        const TWorkloadDescriptor& workloadDescriptor) override;

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

    ISortedStorePtr BackingStore_;

    TPreloadedBlockCachePtr PreloadedBlockCache_;

    EInMemoryMode InMemoryMode_ = EInMemoryMode::None;

    const NTableClient::TKeyComparer KeyComparer_;

    const bool RequireChunkPreload_;

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
    ISortedStorePtr GetBackingStore();
    NChunkClient::IBlockCachePtr GetBlockCache();

    void PrecacheProperties();

    void OnLocalReaderFailed();
    void OnChunkExpired();
    void OnChunkReaderExpired();

    bool ValidateBlockCachePreloaded();
};

DEFINE_REFCOUNTED_TYPE(TSortedChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
