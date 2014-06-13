#include "stdafx.h"
#include "chunk_replica.h"
#include "chunk.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_replica.h>

#include <server/node_tracker_server/node.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TNodePtrWithIndex value)
{
    return Sprintf("%s/%d", ~value.GetPtr()->GetAddress(), value.GetIndex());
}

Stroka ToString(TChunkPtrWithIndex value)
{
    auto* chunk = value.GetPtr();
    int index = value.GetIndex();
    if (chunk->IsErasure() && index != GenericChunkIndex) {
        return Sprintf("%s/%d",
            ~ToString(chunk->GetId()),
            index);
    } else if (chunk->IsJournal()) {
        return Sprintf("%s/%s",
            ~ToString(chunk->GetId()),
            index == SealedChunkIndex ? "sealed" : "unsealed");
    } else {
        return ToString(chunk->GetId());
    }
}

void ToProto(ui32* protoValue, TNodePtrWithIndex value)
{
    NChunkClient::TChunkReplica clientReplica(
        value.GetPtr()->GetId(),
        value.GetIndex());
    NChunkClient::ToProto(protoValue, clientReplica);
}

TChunkId EncodeChunkId(TChunkPtrWithIndex chunkWithIndex)
{
    auto* chunk = chunkWithIndex.GetPtr();
    int index = chunkWithIndex.GetIndex();
    return chunk->IsErasure() && index != GenericChunkIndex
        ? ErasurePartIdFromChunkId(chunk->GetId(), index)
        : chunk->GetId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
