#pragma once

#include "public.h"
#include "store_detail.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkStore
    : public TChunkStoreBase
    , public TSortedStoreBase
{
public:
    TSortedChunkStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet,
        NChunkClient::IBlockCachePtr blockCache,
        NDataNode::TChunkRegistryPtr chunkRegistry = nullptr,
        NDataNode::TChunkBlockManagerPtr chunkBlockManager = nullptr,
        TVersionedChunkMetaManagerPtr chunkMetaManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr,
        const NNodeTrackerClient::TNodeDescriptor& localDescriptor = NNodeTrackerClient::TNodeDescriptor());
    ~TSortedChunkStore();

    // IStore implementation.
    virtual TSortedChunkStorePtr AsSortedChunk() override;

    // IChunkStore implementation.
    virtual EStoreType GetType() const override;

    // ISortedStore implementation.
    virtual TOwningKey GetMinKey() const override;
    virtual TOwningKey GetMaxKey() const override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<NTableClient::TRowRange> bounds,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions) override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions) override;

    virtual TError CheckRowLocks(
        TUnversionedRow row,
        TTransaction* transaction,
        ui32 lockMask) override;

private:
    // Cached for fast retrieval from ChunkMeta_.
    TOwningKey MinKey_;
    TOwningKey MaxKey_;

    const NTableClient::TKeyComparer KeyComparer_;

    NTableClient::IVersionedReaderPtr CreateCacheBasedReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const TTableSchema& schema);
    NTableClient::IVersionedReaderPtr CreateCacheBasedReader(
        TSharedRange<NTableClient::TRowRange> bounds,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const TTableSchema& schema);

    NTableClient::TChunkStatePtr PrepareChunkState(
        NChunkClient::IChunkReaderPtr chunkReader,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions);

    void ValidateBlockSize(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NTableClient::TChunkStatePtr& chunkState,
        const TWorkloadDescriptor& workloadDescriptor);

    virtual void PrecacheProperties() override;

    virtual NTableClient::TKeyComparer GetKeyComparer() override;

    ISortedStorePtr GetSortedBackingStore();
};

DEFINE_REFCOUNTED_TYPE(TSortedChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
