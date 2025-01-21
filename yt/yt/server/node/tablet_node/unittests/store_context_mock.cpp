#include "store_context_mock.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TStoreContextMock::TStoreContextMock()
    : BlockCache_(
        CreateClientBlockCache(
            New<TBlockCacheConfig>(),
            EBlockType::UncompressedData,
            GetNullMemoryUsageTracker()))
    , VersionedChunkMetaManager_(
        CreateVersionedChunkMetaManager(
            New<TSlruCacheConfig>(),
            GetNullMemoryUsageTracker()))
    , ColumnEvaluatorCache_(
        CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>()))
{ }

const NChunkClient::IBlockCachePtr& TStoreContextMock::GetBlockCache()
{
    return BlockCache_;
}

const IVersionedChunkMetaManagerPtr& TStoreContextMock::GetVersionedChunkMetaManager()
{
    return VersionedChunkMetaManager_;
}

const NQueryClient::IColumnEvaluatorCachePtr& TStoreContextMock::GetColumnEvaluatorCache()
{
    return ColumnEvaluatorCache_;
}

const TTabletManagerConfigPtr& TStoreContextMock::GetTabletManagerConfig()
{
    static const auto result = New<TTabletManagerConfig>();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
