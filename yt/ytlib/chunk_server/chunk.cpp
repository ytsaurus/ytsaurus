#include "stdafx.h"
#include "chunk.h"

namespace NYT {
namespace NChunkServer {

using namespace NChunkHolder::NProto;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(const TChunkId& id)
    : Id_(id)
    , RefCounter(0)
{ }

TChunk::TChunk(const TChunk& other)
    : Id_(other.Id_)
    , ChunkListId_(other.ChunkListId_)
    , Attributes_(Attributes_)
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
    ::Save(output, Attributes_);
    ::Save(output, Locations_);
    ::Save(output, RefCounter);

}

TAutoPtr<TChunk> TChunk::Load(const TChunkId& id, TInputStream* input)
{
    TAutoPtr<TChunk> chunk = new TChunk(id);
    ::Load(input, chunk->ChunkListId_);
    ::Load(input, chunk->Attributes_);
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

TChunkAttributes TChunk::DeserializeAttributes() const
{
    YASSERT(IsConfirmed());
    TChunkAttributes attributes;
    // Deserialize the blob received from the holders.
    if (!DeserializeProtobuf(&attributes, Attributes_)) {
        LOG_FATAL("Error deserializing chunk attributes (ChunkId: %s)", ~Id_.ToString());
    }
    return attributes;
}

bool TChunk::IsConfirmed() const
{
    return Attributes_ != TSharedRef();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
