#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/chunk_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusher
    : public TRefCounted
{
public:
    TStoreFlusher(
        TStoreFlusherConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TStoreFlusher();

    TFuture<NChunkClient::TChunkId> Enqueue(TDynamicMemoryStorePtr store, int storeIndex);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
