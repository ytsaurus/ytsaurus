#include "chunk_replica.h"
#include "chunk.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/string.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TString ToString(TNodePtrWithIndexes value)
{
    if (value.GetReplicaIndex() == GenericChunkReplicaIndex) {
        return Format("%v@%v",
            value.GetPtr()->GetDefaultAddress(),
            value.GetMediumIndex());
    } else {
        return Format("%v/%v@%v",
            value.GetPtr()->GetDefaultAddress(),
            value.GetReplicaIndex(),
            value.GetMediumIndex());
    }
}

TString ToString(TChunkPtrWithIndexes value)
{
    auto* chunk = value.GetPtr();
    const auto& id = chunk->GetId();
    int replicaIndex = value.GetReplicaIndex();
    int mediumIndex = value.GetMediumIndex();
    if (chunk->IsJournal()) {
        return Format("%v/%v@%v",
            id,
            EJournalReplicaType(replicaIndex),
            mediumIndex);
    } else if (replicaIndex != GenericChunkReplicaIndex) {
        return Format("%v/%v@%v",
            id,
            replicaIndex,
            mediumIndex);
    } else {
        return Format("%v@%v",
            id,
            mediumIndex);
    }
}

void ToProto(ui32* protoValue, TNodePtrWithIndexes value)
{
    NChunkClient::TChunkReplica clientReplica(
        value.GetPtr()->GetId(),
        value.GetReplicaIndex(),
        value.GetMediumIndex());
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
