#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkMetaManager
    : public virtual TRefCounted
{
    virtual void Reconfigure(TSlruCacheDynamicConfigPtr config) = 0;

    virtual TFuture<NTableClient::TCachedVersionedChunkMetaPtr> GetMeta(
        const NChunkClient::IChunkReaderPtr& chunkReader,
        const NTableClient::TTableSchemaPtr& schema,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedChunkMetaManager)

IVersionedChunkMetaManagerPtr CreateVersionedChunkMetaManager(
    TSlruCacheConfigPtr config,
    NClusterNode::IBootstrapBase* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
