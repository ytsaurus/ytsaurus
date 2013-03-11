#include "stdafx.h"
#include "chunk_replica.h"
#include "node.h"

#include <ytlib/chunk_client/chunk_replica.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TDataNodeWithIndex replica)
{
    return Sprintf("%s/%d", ~replica.GetPtr()->GetAddress(), replica.GetIndex());
}

void ToProto(ui32* protoValue, TDataNodeWithIndex value)
{
    NChunkClient::TChunkReplica clientReplica(
        value.GetPtr()->GetId(),
        value.GetIndex());
    NChunkClient::ToProto(protoValue, clientReplica);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
