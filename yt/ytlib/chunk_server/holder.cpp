#include "stdafx.h"
#include "holder.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

THolder::THolder(
    THolderId id,
    const Stroka& address,
    EHolderState state,
    const NChunkHolder::THolderStatistics& statistics)
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
    , ChunkIds_(other.ChunkIds_)
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
    ::Save(output, Statistics_);
    SaveSet(output, ChunkIds_);
    ::Save(output, JobIds_);
}

TAutoPtr<THolder> THolder::Load(THolderId id, TInputStream* input)
{
    Stroka address;
    EHolderState state;
    NChunkHolder::THolderStatistics statistics;
    ::Load(input, address);
    ::Load(input, state);
    ::Load(input, statistics);
    TAutoPtr<THolder> holder = new THolder(id, address, state, statistics);
    ::Load(input, holder->ChunkIds_);
    ::Load(input, holder->JobIds_);
    return holder;
}

void THolder::AddJob(const NChunkHolder::TJobId& id)
{
    JobIds_.push_back(id);
}

void THolder::RemoveJob(const NChunkHolder::TJobId& id)
{
    auto it = std::find(JobIds_.begin(), JobIds_.end(), id);
    if (it != JobIds_.end()) {
        JobIds_.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
