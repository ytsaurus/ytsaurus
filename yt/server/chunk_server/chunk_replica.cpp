#include "stdafx.h"
#include "chunk_replica.h"
#include "node.h"
#include "chunk.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_replica.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TDataNodePtrWithIndex value)
{
    return Sprintf("%s/%d", ~value.GetPtr()->GetAddress(), value.GetIndex());
}

Stroka ToString(TChunkPtrWithIndex value)
{
    return Sprintf("%s/%d", ~ToString(value.GetPtr()->GetId()), value.GetIndex());
}

void ToProto(ui32* protoValue, TDataNodePtrWithIndex value)
{
    NChunkClient::TChunkReplica clientReplica(
        value.GetPtr()->GetId(),
        value.GetIndex());
    NChunkClient::ToProto(protoValue, clientReplica);
}

TChunkId EncodeChunkId(TChunkPtrWithIndex chunkWithIndex)
{
    auto* chunk = chunkWithIndex.GetPtr();
    return chunk->IsErasure()
           ? PartIdFromErasureChunkId(chunk->GetId(), chunkWithIndex.GetIndex())
           : chunk->GetId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
