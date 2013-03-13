#include "stdafx.h"
#include "node.h"
#include "job.h"
#include "chunk.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TDataNode::TDataNode(
    TNodeId id,
    const TNodeDescriptor& descriptor,
    const TIncarnationId& incarnationId)
    : Id_(id)
    , IncarnationId_(incarnationId)
    , Descriptor_(descriptor)
{
    Init();
}

TDataNode::TDataNode(TNodeId id)
    : Id_(id)
{
    Init();
}

void TDataNode::Init()
{
    ChunksToReplicate_.resize(ReplicationPriorityCount);
    HintedSessionCount_ = 0;
}

const TNodeDescriptor& TDataNode::GetDescriptor() const
{
    return Descriptor_;
}

const Stroka& TDataNode::GetAddress() const
{
    return Descriptor_.Address;
}

void TDataNode::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    ::Save(output, Descriptor_.Address);
    ::Save(output, IncarnationId_);
    ::Save(output, State_);
    SaveProto(output, Statistics_);
    SaveObjectRefs(context, StoredReplicas_);
    SaveObjectRefs(context, CachedReplicas_);
    SaveObjectRefs(context, UnapprovedReplicas_);
    SaveObjectRefs(context, Jobs_);
}

void TDataNode::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, Descriptor_.Address);
    ::Load(input, IncarnationId_);
    ::Load(input, State_);
    LoadProto(input, Statistics_);
    LoadObjectRefs(context, StoredReplicas_);
    LoadObjectRefs(context, CachedReplicas_);
    LoadObjectRefs(context, UnapprovedReplicas_);
    LoadObjectRefs(context, Jobs_);
}

void TDataNode::AddJob(TJob* job)
{
    Jobs_.push_back(job);
}

void TDataNode::RemoveJob(TJob* job)
{
    auto it = std::find(Jobs_.begin(), Jobs_.end(), job);
    if (it != Jobs_.end()) {
        Jobs_.erase(it);
    }
}

void TDataNode::AddReplica(TChunkPtrWithIndex replica, bool cached)
{
    if (cached) {
        YCHECK(CachedReplicas_.insert(replica).second);
    } else {
        YCHECK(StoredReplicas_.insert(replica).second);
    }
}

void TDataNode::RemoveReplica(TChunkPtrWithIndex replica, bool cached)
{
    if (cached) {
        YCHECK(CachedReplicas_.erase(replica) == 1);
    } else {
        YCHECK(StoredReplicas_.erase(replica) == 1);
        UnapprovedReplicas_.erase(replica);
    }
}

bool TDataNode::HasReplica(TChunkPtrWithIndex replica, bool cached) const
{
    if (cached) {
        return CachedReplicas_.find(replica) != CachedReplicas_.end();
    } else {
        return StoredReplicas_.find(replica) != StoredReplicas_.end();
    }
}

void TDataNode::MarkReplicaUnapproved(TChunkPtrWithIndex replica)
{
    YASSERT(HasReplica(replica, false));
    YCHECK(UnapprovedReplicas_.insert(replica).second);
}

bool TDataNode::HasUnapprovedReplica(TChunkPtrWithIndex replica) const
{
    return UnapprovedReplicas_.find(replica) != UnapprovedReplicas_.end();
}

void TDataNode::ApproveReplica(TChunkPtrWithIndex replica)
{
    YASSERT(HasReplica(replica, false));
    YCHECK(UnapprovedReplicas_.erase(replica) == 1);
}

int TDataNode::GetTotalSessionCount() const
{
    return HintedSessionCount_ + Statistics_.total_session_count();
}

TNodeId GetObjectId(const TDataNode* node)
{
    return node->GetId();
}

bool CompareObjectsForSerialization(const TDataNode* lhs, const TDataNode* rhs)
{
    return GetObjectId(lhs) < GetObjectId(rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

