#include "stdafx.h"
#include "chunk_replica.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/erasure/public.h>
        
#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NChunkClient {

using namespace NNodeTrackerClient;

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
    YASSERT(index >= 0 && index < NErasure::MaxTotalPartCount);
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
    return Sprintf("%d/%d", replica.GetNodeId(), replica.GetIndex());
}

TChunkReplicaAddressFormatter::TChunkReplicaAddressFormatter(TNodeDirectoryPtr nodeDirectory)
    : NodeDirectory(nodeDirectory)
{ }

Stroka TChunkReplicaAddressFormatter::Format(TChunkReplica replica) const
{
    const auto& descriptor = NodeDirectory->GetDescriptor(replica.GetNodeId());
    return Sprintf("%s/%d", ~descriptor.Address, replica.GetIndex());
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NChunkClient
} // namespace NYT
