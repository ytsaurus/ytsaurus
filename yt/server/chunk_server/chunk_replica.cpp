#include "stdafx.h"
#include "chunk_replica.h"
#include "node.h"
#include "chunk.h"

#include <ytlib/chunk_client/chunk_replica.h>

namespace NYT {
namespace NChunkServer {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
