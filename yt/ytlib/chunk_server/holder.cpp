#include "stdafx.h"
#include "holder.h"

#include <ytlib/misc/assert.h>
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
    const TIncarnationId& incarnationId,
    EHolderState state,
    const THolderStatistics& statistics)
    : Id_(id)
    , Address_(address)
    , IncarnationId_(incarnationId)
    , State_(state)
    , Statistics_(statistics)
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
    SaveSet(output, StoredChunkIds_);
    SaveSet(output, CachedChunkIds_);
    SaveSet(output, UnapprovedChunkIds_);
    ::Save(output, JobIds_);
}

void THolder::Load(TInputStream* input, const TLoadContext& context)
{
    UNUSED(context);
    ::Load(input, Address_);
    ::Load(input, IncarnationId_);
    ::Load(input, State_);
    LoadProto(input, Statistics_);
    LoadSet(input, StoredChunkIds_);
    LoadSet(input, CachedChunkIds_);
    LoadSet(input, UnapprovedChunkIds_);
    ::Load(input, JobIds_);
}

void THolder::AddJob(const TJobId& id)
{
    JobIds_.push_back(id);
}

void THolder::RemoveJob(const TJobId& id)
{
    auto it = std::find(JobIds_.begin(), JobIds_.end(), id);
    if (it != JobIds_.end()) {
        JobIds_.erase(it);
    }
}

void THolder::AddChunk(const TChunkId& chunkId, bool cached)
{
    if (cached) {
        YVERIFY(CachedChunkIds_.insert(chunkId).second);
    } else {
        YVERIFY(StoredChunkIds_.insert(chunkId).second);
    }
}

void THolder::RemoveChunk(const TChunkId& chunkId, bool cached)
{
    if (cached) {
        YVERIFY(CachedChunkIds_.erase(chunkId) == 1);
    } else {
        YVERIFY(StoredChunkIds_.erase(chunkId) == 1);
    }
}

bool THolder::HasChunk(const TChunkId& chunkId, bool cached) const
{
    if (cached) {
        return CachedChunkIds_.find(chunkId) != CachedChunkIds_.end();
    } else {
        return StoredChunkIds_.find(chunkId) != StoredChunkIds_.end();
    }
}

void THolder::AddUnapprovedChunk(const TChunkId& chunkId)
{
    if (!HasChunk(chunkId, false)) {
        AddChunk(chunkId, false);
        YVERIFY(UnapprovedChunkIds_.insert(chunkId).second);
    }
}

void THolder::RemoveUnapprovedChunk(const TChunkId& chunkId)
{
    YVERIFY(UnapprovedChunkIds_.erase(chunkId) == 1);
    RemoveChunk(chunkId, false);
}

bool THolder::HasUnapprovedChunk(const TChunkId& chunkId) const
{
    return UnapprovedChunkIds_.find(chunkId) != UnapprovedChunkIds_.end();
}

void THolder::ApproveChunk(const TChunkId& chunkId)
{
    YVERIFY(UnapprovedChunkIds_.erase(chunkId) == 1);
    YASSERT(HasChunk(chunkId, false));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

