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
    , ChunkListId_(NullChunkListId)
    , Size_(UnknownSize)
    , RefCounter(0)
{ }

TChunk::TChunk(const TChunk& other)
    : Id_(other.Id_)
    , ChunkListId_(other.ChunkListId_)
    , Size_(other.Size_)
    , Attributes_(Attributes_)
    , StoredLocations_(other.StoredLocations_)
    , RefCounter(other.RefCounter)
{
    if (~other.CachedLocations_ != NULL) {
        CachedLocations_ = new yhash_set<THolderId>(*other.CachedLocations_);
    }
}

TAutoPtr<TChunk> TChunk::Clone() const
{
    return new TChunk(*this);
}

void TChunk::Save(TOutputStream* output) const
{
    ::Save(output, ChunkListId_);
    ::Save(output, Size_);
    ::Save(output, Attributes_);
    ::Save(output, StoredLocations_);
    SaveNullableSet(output, CachedLocations_);
    ::Save(output, RefCounter);

}

TAutoPtr<TChunk> TChunk::Load(const TChunkId& id, TInputStream* input)
{
    TAutoPtr<TChunk> chunk = new TChunk(id);
    ::Load(input, chunk->ChunkListId_);
    ::Load(input, chunk->Size_);
    ::Load(input, chunk->Attributes_);
    ::Load(input, chunk->StoredLocations_);
    LoadNullableSet(input, chunk->CachedLocations_);
    ::Load(input, chunk->RefCounter);
    return chunk;
}

void TChunk::AddLocation(THolderId holderId, bool cached)
{
    if (cached) {
        if (~CachedLocations_ == NULL) {
            CachedLocations_ = new yhash_set<THolderId>();
        }
        YVERIFY(CachedLocations_->insert(holderId).second);
    } else {
        StoredLocations_.push_back(holderId);
    }
}

void TChunk::RemoveLocation(THolderId holderId, bool cached)
{
    if (cached) {
        YASSERT(~CachedLocations_);
        YVERIFY(CachedLocations_->erase(holderId) == 1);
    } else {
        for (auto it = StoredLocations_.begin(); it != StoredLocations_.end(); ++it) {
            if (*it == holderId) {
                StoredLocations_.erase(it);
                return;
            }
        }
        YUNREACHABLE();
    }
}

yvector<THolderId> TChunk::GetLocations() const
{
    yvector<THolderId> result(StoredLocations_);
    if (~CachedLocations_ != NULL) {
        result.insert(result.end(), CachedLocations_->begin(), CachedLocations_->end());
    }
    return result;
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
