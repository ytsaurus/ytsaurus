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

////////////////////////////////////////////////////////////////////////////////

TDataNode::TDataNode(
    TNodeId id,
    const Stroka& address,
    const TIncarnationId& incarnationId)
    : Id_(id)
    , Address_(address)
    , IncarnationId_(incarnationId)
{ }

TDataNode::TDataNode(TNodeId id)
    : Id_(id)
{ }

void TDataNode::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    ::Save(output, Address_);
    ::Save(output, IncarnationId_);
    ::Save(output, State_);
    SaveProto(output, Statistics_);
    SaveObjectRefs(output, StoredChunks_);
    SaveObjectRefs(output, CachedChunks_);
    SaveObjectRefs(output, UnapprovedChunks_);
    SaveObjectRefs(output, Jobs_);
}

void TDataNode::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, Address_);
    ::Load(input, IncarnationId_);
    ::Load(input, State_);
    LoadProto(input, Statistics_);
    LoadObjectRefs(input, StoredChunks_, context);
    LoadObjectRefs(input, CachedChunks_, context);
    LoadObjectRefs(input, UnapprovedChunks_, context);
    LoadObjectRefs(input, Jobs_, context);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

