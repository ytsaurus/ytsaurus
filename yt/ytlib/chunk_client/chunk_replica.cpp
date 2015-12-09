#include "chunk_replica.h"

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/format.h>

namespace NYT {
namespace NChunkClient {

using namespace NNodeTrackerClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TChunkReplica replica)
{
    if (replica.GetIndex() == GenericChunkReplicaIndex) {
        return ToString(replica.GetNodeId());
    } else {
        return Format("%v/%v",
            replica.GetNodeId(),
            replica.GetIndex());
    }
}

Stroka ToString(const TChunkIdWithIndex& id)
{
    if (TypeFromId(id.Id) == EObjectType::JournalChunk) {
        return Format("%v/%v",
            id.Id,
            EJournalReplicaType(id.Index));
    } else if (id.Index != GenericChunkReplicaIndex) {
        return Format("%v/%v",
            id.Id,
            id.Index);
    } else {
        return ToString(id.Id);
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicaAddressFormatter::TChunkReplicaAddressFormatter(TNodeDirectoryPtr nodeDirectory)
    : NodeDirectory_(nodeDirectory)
{ }

Stroka TChunkReplicaAddressFormatter::operator () (TChunkReplica replica) const
{
    const auto& descriptor = NodeDirectory_->GetDescriptor(replica.GetNodeId());
    return Format("%v/%v", descriptor.GetDefaultAddress(), replica.GetIndex());
}

////////////////////////////////////////////////////////////////////////////////

bool IsArtifactChunkId(const TChunkId& id)
{
    return TypeFromId(id) == EObjectType::Artifact;
}
bool IsErasureChunkId(const TChunkId& id)
{
    return TypeFromId(id) == EObjectType::ErasureChunk;
}

bool IsErasureChunkPartId(const TChunkId& id)
{
    auto type = TypeFromId(id);
    return
        type >= EObjectType::ErasureChunkPart_0 &&
        type <= EObjectType::ErasureChunkPart_15;
}

TChunkId ErasurePartIdFromChunkId(const TChunkId& id, int index)
{
    return ReplaceTypeInId(id, EObjectType(static_cast<int>(EObjectType::ErasureChunkPart_0) + index));
}

TChunkId ErasureChunkIdFromPartId(const TChunkId& id)
{
    return ReplaceTypeInId(id, EObjectType::ErasureChunk);
}

int IndexFromErasurePartId(const TChunkId& id)
{
    int index = static_cast<int>(TypeFromId(id)) - static_cast<int>(EObjectType::ErasureChunkPart_0);
    YCHECK(index >= 0 && index <= 15);
    return index;
}

TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex)
{
    return IsErasureChunkId(idWithIndex.Id)
        ? ErasurePartIdFromChunkId(idWithIndex.Id, idWithIndex.Index)
        : idWithIndex.Id;
}

TChunkIdWithIndex DecodeChunkId(const TChunkId& id)
{
    if (IsErasureChunkId(id)) {
        return TChunkIdWithIndex(id, AllChunkReplicasIndex);
    } else if (IsErasureChunkPartId(id)) {
        return TChunkIdWithIndex(ErasureChunkIdFromPartId(id), IndexFromErasurePartId(id));
    } else {
        return TChunkIdWithIndex(id, GenericChunkReplicaIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
