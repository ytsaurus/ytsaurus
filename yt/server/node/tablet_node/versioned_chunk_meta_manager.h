#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkMetaManager
    : public TRefCounted
{
public:
    TVersionedChunkMetaManager(
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);

    ~TVersionedChunkMetaManager();

    TFuture<NTableClient::TCachedVersionedChunkMetaPtr> GetMeta(
        NChunkClient::IChunkReaderPtr chunkReader,
        const NTableClient::TTableSchema& schema,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};


DEFINE_REFCOUNTED_TYPE(TVersionedChunkMetaManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
