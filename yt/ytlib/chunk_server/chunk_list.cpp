#include "stdafx.h"
#include "chunk_list.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : Id_(id)
    , ReplicaCount_(3)
    , RefCounter(0)
{ }

TChunkList::TChunkList(const TChunkList& other)
    : Id_(other.Id_)
    , ChunkIds_(other.ChunkIds_)
    , ReplicaCount_(other.ReplicaCount_)
    , RefCounter(other.RefCounter)
{ }

TAutoPtr<TChunkList> TChunkList::Clone() const
{
    return new TChunkList(*this);
}

void TChunkList::Save(TOutputStream* output) const
{
    ::Save(output, ChunkIds_);
    ::Save(output, ReplicaCount_);
    ::Save(output, RefCounter);
}

TAutoPtr<TChunkList> TChunkList::Load(const TChunkListId& id, TInputStream* input)
{
    TAutoPtr<TChunkList> chunkList = new TChunkList(id);
    ::Load(input, chunkList->ChunkIds_);
    ::Load(input, chunkList->ReplicaCount_);
    ::Load(input, chunkList->RefCounter);
    return chunkList;
}

i32 TChunkList::Ref()
{
    return ++RefCounter;
}

i32 TChunkList::Unref()
{
    return --RefCounter;
}

i32 TChunkList::GetRefCounter() const
{
    return RefCounter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
