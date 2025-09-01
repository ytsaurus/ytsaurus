#include "chunk_replica.h"
#include "chunk.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NCellMaster;
using namespace NNodeTrackerClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaIndex value, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", value.GetPtr()->GetId());
    if (value.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", value.GetReplicaIndex());
    }
}

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaInfo value, TStringBuf spec)
{
    FormatValue(builder, TChunkPtrWithReplicaIndex(value.GetPtr(), value.GetReplicaIndex()), spec);
    if (value.GetReplicaState() != EChunkReplicaState::Generic) {
        builder->AppendFormat(":%v", value.GetReplicaState());
    }
}

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaAndMediumIndex value, TStringBuf spec)
{
    FormatValue(builder, TChunkPtrWithReplicaIndex(value.GetPtr(), value.GetReplicaIndex()), spec);
    if (value.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (value.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", value.GetMediumIndex());
    }
}

void FormatValue(TStringBuilderBase* builder, TChunkLocationPtrWithReplicaIndex value, TStringBuf /*spec*/)
{
    auto* location = value.GetPtr();
    builder->AppendFormat("%v(%v)",
        location->GetNode()->GetId(),
        location->GetUuid());
    if (value.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", value.GetReplicaIndex());
    }
}
void FormatValue(TStringBuilderBase* builder, TChunkLocationPtrWithReplicaInfo value, TStringBuf spec)
{
    FormatValue(builder, TChunkLocationPtrWithReplicaIndex(value.GetPtr(), value.GetReplicaIndex()), spec);
    if (value.GetReplicaState() != EChunkReplicaState::Generic) {
        builder->AppendFormat(":%v", value.GetReplicaState());
    }
}

void ToProto(ui64* protoValue, TNodePtrWithReplicaAndMediumIndex value)
{
    TChunkReplicaWithMedium replica(
        value.GetPtr()->GetId(),
        value.GetReplicaIndex(),
        value.GetMediumIndex());
    NChunkClient::ToProto(protoValue, replica);
}

void ToProto(ui32* protoValue, TNodePtrWithReplicaAndMediumIndex value)
{
    TChunkReplica replica(
        value.GetPtr()->GetId(),
        value.GetReplicaIndex());
    NChunkClient::ToProto(protoValue, replica);
}

void ToProto(ui32* protoValue, TNodePtrWithReplicaIndex value)
{
    TChunkReplica replica(value.GetPtr()->GetId(), value.GetReplicaIndex());
    NChunkClient::ToProto(protoValue, replica);
}

void ToProto(ui64* protoValue, TChunkLocationPtrWithReplicaIndex value)
{
    TNodePtrWithReplicaAndMediumIndex replica(
        value.GetPtr()->GetNode(),
        value.GetReplicaIndex(),
        value.GetPtr()->GetEffectiveMediumIndex());
    ToProto(protoValue, replica);
}

void ToProto(ui64* protoValue, TChunkLocationPtrWithReplicaInfo value)
{
    ToProto(protoValue, TChunkLocationPtrWithReplicaIndex(value.GetPtr(), value.GetReplicaIndex()));
}

void ToProto(ui32* protoValue, TChunkLocationPtrWithReplicaIndex value)
{
    TNodePtrWithReplicaIndex replica(value.GetPtr()->GetNode(), value.GetReplicaIndex());
    ToProto(protoValue, replica);
}

void ToProto(ui32* protoValue, TChunkLocationPtrWithReplicaInfo value)
{
    ToProto(protoValue, TChunkLocationPtrWithReplicaIndex(value.GetPtr(), value.GetReplicaIndex()));
}

////////////////////////////////////////////////////////////////////////////////

TChunkIdWithIndex ToChunkIdWithIndex(TChunkPtrWithReplicaIndex chunkWithIndex)
{
    auto* chunk = chunkWithIndex.GetPtr();
    YT_VERIFY(chunk);
    return TChunkIdWithIndex(chunk->GetId(), chunkWithIndex.GetReplicaIndex());
}

TChunkIdWithIndexes ToChunkIdWithIndexes(TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    YT_VERIFY(chunk);
    return {chunk->GetId(), chunkWithIndexes.GetReplicaIndex(), chunkWithIndexes.GetMediumIndex()};
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaChunkReplica::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, ChunkId);
    Persist(context, ReplicaIndex);
    Persist(context, NodeId);
    Persist(context, LocationIndex);
}

void FromProto(TSequoiaChunkReplica* replica, const NProto::TSequoiaReplicaInfo& protoReplica)
{
    replica->NodeId = FromProto<TNodeId>(protoReplica.node_id());
    replica->ReplicaIndex = protoReplica.replica_index();
    replica->ChunkId = FromProto<TChunkId>(protoReplica.chunk_id());
    replica->LocationIndex = FromProto<TChunkLocationIndex>(protoReplica.location_index());
}

void ToProto(NProto::TSequoiaReplicaInfo* protoReplica, const TSequoiaChunkReplica& replica)
{
    protoReplica->set_node_id(ToProto(replica.NodeId));
    protoReplica->set_replica_index(replica.ReplicaIndex);
    ToProto(protoReplica->mutable_chunk_id(), replica.ChunkId);
    protoReplica->set_location_index(ToProto(replica.LocationIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
