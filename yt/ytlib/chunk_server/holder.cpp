#include "stdafx.h"
#include "holder.h"
#include "job.h"
#include "chunk.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/cell_master/load_context.h>

namespace NYT {

namespace NChunkServer {

using namespace NProto;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

THolder::THolder(
    THolderId id,
    const Stroka& address,
    const TIncarnationId& incarnationId)
    : Id_(id)
    , Address_(address)
    , IncarnationId_(incarnationId)
{ }

THolder::THolder(THolderId id)
    : Id_(id)
{ }

void THolder::Save(TOutputStream* output) const
{
    ::Save(output, Address_);
    ::Save(output, IncarnationId_);
    ::Save(output, State_);
    SaveProto(output, Statistics_);
    SaveObjectRefs(output, StoredChunks_);
    SaveObjectRefs(output, CachedChunks_);
    SaveObjectRefs(output, UnapprovedChunks_);
    SaveObjectRefs(output, Jobs_);
}

void THolder::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    ::Load(input, Address_);
    ::Load(input, IncarnationId_);
    ::Load(input, State_);
    LoadProto(input, Statistics_);
    LoadObjectRefs(input, StoredChunks_, context);
    LoadObjectRefs(input, CachedChunks_, context);
    LoadObjectRefs(input, UnapprovedChunks_, context);
    LoadObjectRefs(input, Jobs_, context);
}

void THolder::AddJob(TJob* job)
{
    Jobs_.push_back(job);
}

void THolder::RemoveJob(TJob* job)
{
    auto it = std::find(Jobs_.begin(), Jobs_.end(), job);
    if (it != Jobs_.end()) {
        Jobs_.erase(it);
    }
}

void THolder::AddChunk(TChunk* chunk, bool cached)
{
    if (cached) {
        YVERIFY(CachedChunks_.insert(chunk).second);
    } else {
        YVERIFY(StoredChunks_.insert(chunk).second);
    }
}

void THolder::RemoveChunk(TChunk* chunk, bool cached)
{
    if (cached) {
        YVERIFY(CachedChunks_.erase(chunk) == 1);
    } else {
        YVERIFY(StoredChunks_.erase(chunk) == 1);
        UnapprovedChunks_.erase(chunk);
    }
}

bool THolder::HasChunk(TChunk* chunk, bool cached) const
{
    if (cached) {
        return CachedChunks_.find(chunk) != CachedChunks_.end();
    } else {
        return StoredChunks_.find(chunk) != StoredChunks_.end();
    }
}

void THolder::MarkChunkUnapproved(TChunk* chunk)
{
    YASSERT(HasChunk(chunk, false));
    YVERIFY(UnapprovedChunks_.insert(chunk).second);
}

bool THolder::HasUnapprovedChunk(TChunk* chunk) const
{
    return UnapprovedChunks_.find(chunk) != UnapprovedChunks_.end();
}

void THolder::ApproveChunk(TChunk* chunk)
{
    YASSERT(HasChunk(chunk, false));
    YVERIFY(UnapprovedChunks_.erase(chunk) == 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

