#include "public.h"

#include "node.h"

#include <yt/yt/server/master/chunk_server/chunk_replica.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

void ToProto(ui64* protoValue, TNodePtrWithIndexes value)
{
    YT_ASSERT(value.GetState() == EChunkReplicaState::Generic);
    NChunkClient::TChunkReplicaWithMedium clientReplica(
        value.GetPtr()->GetId(),
        value.GetReplicaIndex(),
        value.GetMediumIndex());
    NChunkClient::ToProto(protoValue, clientReplica);
}

void ToProto(ui32* protoValue, TNodePtrWithIndexes value)
{
    YT_ASSERT(value.GetState() == EChunkReplicaState::Generic);
    NChunkClient::TChunkReplica clientReplica(
        value.GetPtr()->GetId(),
        value.GetReplicaIndex());
    NChunkClient::ToProto(protoValue, clientReplica);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
