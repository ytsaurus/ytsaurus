#include "chunk_replica.h"
#include "chunk.h"

#include <yt/server/master/node_tracker_server/node.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/misc/string.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TNodePtrWithIndexes replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.GetPtr()->GetDefaultAddress());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    if (replica.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.GetMediumIndex());
    }
    if (replica.GetState() != EChunkReplicaState::Generic) {
        builder->AppendFormat(":%v", replica.GetState());
    }
}

TString ToString(TNodePtrWithIndexes value)
{
    return ToStringViaBuilder(value);
}

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithIndexes replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.GetPtr()->GetId());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    if (replica.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.GetMediumIndex());
    }
    if (replica.GetState() != EChunkReplicaState::Generic) {
        builder->AppendFormat(":%v", replica.GetState());
    }
}

TString ToString(TChunkPtrWithIndexes value)
{
    return ToStringViaBuilder(value);
}

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
    NChunkClient::TChunkReplica clientReplica(
        value.GetPtr()->GetId(),
        value.GetReplicaIndex());
    NChunkClient::ToProto(protoValue, clientReplica);
}

TChunkId EncodeChunkId(TChunkPtrWithIndexes chunkWithIndexes)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    return chunk->IsErasure()
        ? ErasurePartIdFromChunkId(chunk->GetId(), chunkWithIndexes.GetReplicaIndex())
        : chunk->GetId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
