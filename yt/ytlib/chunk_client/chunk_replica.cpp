#include "stdafx.h"
#include "chunk_replica.h"

#include <core/misc/format.h>

#include <core/erasure/public.h>
        
#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NNodeTrackerClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TChunkReplica::TChunkReplica()
    : Value(InvalidNodeId | (0 << 28))
{ }

TChunkReplica::TChunkReplica(ui32 value)
    : Value(value)
{ }

TChunkReplica::TChunkReplica(int nodeId, int index)
    : Value(nodeId | (index << 28))
{
    YASSERT(nodeId >= 0 && nodeId <= MaxNodeId);
    YASSERT(index >= 0 && index < ChunkReplicaIndexBound);
}

int TChunkReplica::GetNodeId() const
{
    return Value & 0x0fffffff;
}

int TChunkReplica::GetIndex() const
{
    return Value >> 28;
}

void ToProto(ui32* value, TChunkReplica replica)
{
    *value = replica.Value;
}

void FromProto(TChunkReplica* replica, ui32 value)
{
    replica->Value = value;
}

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TChunkReplica replica)
{
    return Format("%v/%v", replica.GetNodeId(), replica.GetIndex());
}

TChunkReplicaAddressFormatter::TChunkReplicaAddressFormatter(TNodeDirectoryPtr nodeDirectory)
    : NodeDirectory_(nodeDirectory)
{ }

Stroka TChunkReplicaAddressFormatter::operator () (TChunkReplica replica) const
{
    const auto& descriptor = NodeDirectory_->GetDescriptor(replica.GetNodeId());
    return Format("%v/%v", descriptor.GetDefaultAddress(), replica.GetIndex());
}

////////////////////////////////////////////////////////////////////////////////

TChunkIdWithIndex::TChunkIdWithIndex()
    : Index(0)
{ }

TChunkIdWithIndex::TChunkIdWithIndex(const TChunkId& id, int index)
    : Id(id)
    , Index(index)
{ }

bool operator == (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return lhs.Id == rhs.Id && lhs.Index == rhs.Index;
}

bool operator != (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{

    return !(lhs == rhs);
}

Stroka ToString(const TChunkIdWithIndex& id)
{
    return Format("%v/%v", id.Id, id.Index);
}

////////////////////////////////////////////////////////////////////////////////

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
    return ReplaceTypeInId(id, EObjectType(EObjectType::ErasureChunkPart_0 + index));
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
