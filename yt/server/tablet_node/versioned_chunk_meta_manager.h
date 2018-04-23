#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkMetaManager
    : public TRefCounted
{
public:
    TVersionedChunkMetaManager(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    TFuture<NTableClient::TCachedVersionedChunkMetaPtr> GetMeta(
        NChunkClient::IChunkReaderPtr chunkReader,
        const NTableClient::TTableSchema& schema,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TReadSessionId& readSessionId);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};


DEFINE_REFCOUNTED_TYPE(TVersionedChunkMetaManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
