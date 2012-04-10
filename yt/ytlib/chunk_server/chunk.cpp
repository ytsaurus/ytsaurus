#include "stdafx.h"
#include "chunk.h"
#include "common.h"

#include <ytlib/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NChunkHolder::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(const TChunkId& id)
    : TObjectWithIdBase(id)
    , Size_(UnknownSize)
{ }

void TChunk::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, Size_);
    ::Save(output, Attributes_);
    ::Save(output, StoredLocations_);
    SaveNullableSet(output, CachedLocations_);
}

void TChunk::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    TObjectWithIdBase::Load(input);
    ::Load(input, Size_);
    ::Load(input, Attributes_);
    ::Load(input, StoredLocations_);
    LoadNullableSet(input, CachedLocations_);
}

void TChunk::AddLocation(THolderId holderId, bool cached)
{
    if (cached) {
        if (!CachedLocations_) {
            CachedLocations_.Reset(new yhash_set<THolderId>());
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
    yvector<THolderId> result(StoredLocations_.begin(), StoredLocations_.end());
    if (~CachedLocations_) {
        result.insert(result.end(), CachedLocations_->begin(), CachedLocations_->end());
    }
    return result;
}

TChunkAttributes TChunk::DeserializeAttributes() const
{
    YASSERT(IsConfirmed());
    TChunkAttributes attributes;
    if (!DeserializeFromProto(&attributes, Attributes_)) {
        LOG_FATAL("Error deserializing chunk attributes (ChunkId: %s)", ~Id_.ToString());
    }
    return attributes;
}

bool TChunk::IsConfirmed() const
{
    return Attributes_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
