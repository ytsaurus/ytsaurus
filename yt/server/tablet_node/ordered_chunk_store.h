#pragma once

#include "public.h"
#include "store_detail.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkStore
    : public TChunkStoreBase
    , public TOrderedStoreBase
{
public:
    TOrderedChunkStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet,
        NChunkClient::IBlockCachePtr blockCache,
        NDataNode::TChunkRegistryPtr chunkRegistry = nullptr,
        NDataNode::TChunkBlockManagerPtr chunkBlockManager = nullptr,
        NApi::IClientPtr client = nullptr,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& localDescriptor = Null);
    ~TOrderedChunkStore();

    // IStore implementation.
    virtual TOrderedChunkStorePtr AsOrderedChunk() override;

    // IChunkStore implementation.
    virtual EStoreType GetType() const override;

    virtual EInMemoryMode GetInMemoryMode() const override;
    virtual void SetInMemoryMode(EInMemoryMode mode) override;

    virtual void Preload(TInMemoryChunkDataPtr chunkData) override;

    // IOrderedStore implementation.
    virtual NTableClient::ISchemafulReaderPtr CreateReader(
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const NTableClient::TTableSchema& schema,
        const TWorkloadDescriptor& workloadDescriptor) override;

private:
    virtual NChunkClient::IBlockCachePtr GetBlockCache() override;

};

DEFINE_REFCOUNTED_TYPE(TOrderedChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
