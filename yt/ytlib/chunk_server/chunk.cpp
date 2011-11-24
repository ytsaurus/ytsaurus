#include "stdafx.h"
#include "chunk.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(const TChunkId& id)
    : Id_(id)
    , Size_(UnknownSize)
    , RefCounter(0)
{ }

TChunk::TChunk(const TChunk& other)
    : Id_(other.Id_)
    , ChunkListId_(other.ChunkListId_)
    , Size_(other.Size_)
    , Locations_(other.Locations_)
    , RefCounter(other.RefCounter)
{ }

TAutoPtr<TChunk> TChunk::Clone() const
{
    return new TChunk(*this);
}

void TChunk::Save(TOutputStream* output) const
{
    ::Save(output, ChunkListId_);
    ::Save(output, Size_);
    ::Save(output, MasterMeta_);
    ::Save(output, Locations_);
    ::Save(output, RefCounter);

}

TAutoPtr<TChunk> TChunk::Load(const TChunkId& id, TInputStream* input)
{
    TAutoPtr<TChunk> chunk = new TChunk(id);
    ::Load(input, chunk->ChunkListId_);
    ::Load(input, chunk->Size_);
    ::Load(input, chunk->MasterMeta_);
    ::Load(input, chunk->Locations_);
    ::Load(input, chunk->RefCounter);
    return chunk;
}


void TChunk::AddLocation(THolderId holderId)
{
    Locations_.push_back(holderId);
}

void TChunk::RemoveLocation(THolderId holderId)
{
    auto it = std::find(Locations_.begin(), Locations_.end(), holderId);
    YASSERT(it != Locations_.end());
    Locations_.erase(it);
}

i32 TChunk::Ref()
{
    return ++RefCounter;
}

i32 TChunk::Unref()
{
    return --RefCounter;
}

i32 TChunk::GetRefCounter() const
{
    return RefCounter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
