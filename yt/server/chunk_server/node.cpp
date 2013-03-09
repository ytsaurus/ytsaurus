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
    , Descriptor_(descriptor)
    , IncarnationId_(incarnationId)
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
    SaveObjectRefs(context, StoredChunks_);
    SaveObjectRefs(context, CachedChunks_);
    SaveObjectRefs(context, UnapprovedChunks_);
    SaveObjectRefs(context, Jobs_);
}

void TDataNode::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, Descriptor_.Address);
    ::Load(input, IncarnationId_);
    ::Load(input, State_);
    LoadProto(input, Statistics_);
    LoadObjectRefs(context, StoredChunks_);
    LoadObjectRefs(context, CachedChunks_);
    LoadObjectRefs(context, UnapprovedChunks_);
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

void TDataNode::AddChunk(TChunk* chunk, bool cached)
{
    if (cached) {
        YCHECK(CachedChunks_.insert(chunk).second);
    } else {
        YCHECK(StoredChunks_.insert(chunk).second);
    }
}

void TDataNode::RemoveChunk(TChunk* chunk, bool cached)
{
    if (cached) {
        YCHECK(CachedChunks_.erase(chunk) == 1);
    } else {
        YCHECK(StoredChunks_.erase(chunk) == 1);
        UnapprovedChunks_.erase(chunk);
    }
}

bool TDataNode::HasChunk(TChunk* chunk, bool cached) const
{
    if (cached) {
        return CachedChunks_.find(chunk) != CachedChunks_.end();
    } else {
        return StoredChunks_.find(chunk) != StoredChunks_.end();
    }
}

void TDataNode::MarkChunkUnapproved(TChunk* chunk)
{
    YASSERT(HasChunk(chunk, false));
    YCHECK(UnapprovedChunks_.insert(chunk).second);
}

bool TDataNode::HasUnapprovedChunk(TChunk* chunk) const
{
    return UnapprovedChunks_.find(chunk) != UnapprovedChunks_.end();
}

void TDataNode::ApproveChunk(TChunk* chunk)
{
    YASSERT(HasChunk(chunk, false));
    YCHECK(UnapprovedChunks_.erase(chunk) == 1);
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

