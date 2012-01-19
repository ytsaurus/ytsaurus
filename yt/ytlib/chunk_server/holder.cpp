#include "stdafx.h"
#include "holder.h"

namespace NYT {
namespace NChunkServer {

using namespace NChunkHolder;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

THolder::THolder(
    THolderId id,
    const Stroka& address,
    EHolderState state,
    const NChunkServer::NProto::THolderStatistics& statistics)
    : Id_(id)
    , Address_(address)
    , State_(state)
    , Statistics_(statistics)
{ }

THolder::THolder(const THolder& other)
    : Id_(other.Id_)
    , Address_(other.Address_)
    , State_(other.State_)
    , Statistics_(other.Statistics_)
    , StoredChunkIds_(other.StoredChunkIds_)
    , CachedChunkIds_(other.CachedChunkIds_)
    , UnapprovedChunkIds_(other.UnapprovedChunkIds_)
    , JobIds_(other.JobIds_)
{ }

TAutoPtr<THolder> THolder::Clone() const
{
    return new THolder(*this);
}

void THolder::Save(TOutputStream* output) const
{
    ::Save(output, Address_);
    ::Save(output, State_);
    SaveProto(output, Statistics_);
    SaveSet(output, StoredChunkIds_);
    SaveSet(output, CachedChunkIds_);
    SaveSet(output, UnapprovedChunkIds_);
    ::Save(output, JobIds_);
}

TAutoPtr<THolder> THolder::Load(THolderId id, TInputStream* input)
{
    Stroka address;
    EHolderState state;
    THolderStatistics statistics;
    ::Load(input, address);
    ::Load(input, state);
    LoadProto(input, statistics);

    TAutoPtr<THolder> holder = new THolder(id, address, state, statistics);
    LoadSet(input, holder->StoredChunkIds_);
    LoadSet(input, holder->CachedChunkIds_);
    LoadSet(input, holder->UnapprovedChunkIds_);
    ::Load(input, holder->JobIds_);
    return holder;
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
        YVERIFY(UnapprovedChunkIds_.insert(chunkId).Second());
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

