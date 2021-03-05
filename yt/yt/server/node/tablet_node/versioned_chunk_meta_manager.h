#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkMetaManager
    : public virtual TRefCounted
{
    virtual TFuture<NTableClient::TCachedVersionedChunkMetaPtr> GetMeta(
        const NChunkClient::IChunkReaderPtr& chunkReader,
        const NTableClient::TTableSchemaPtr& schema,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedChunkMetaManager)

IVersionedChunkMetaManagerPtr CreateVersionedChunkMetaManager(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
