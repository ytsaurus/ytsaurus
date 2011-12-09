#include "stdafx.h"
#include "chunk.h"
#include "location.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(TLocation* location, const TChunkInfo& info)
    : Id_(TGuid::FromProto(info.id()))
    , Location_(location)
    , Size_(info.size())
    , HasInfo(true)
    , Info(info)
{ }

TChunk::TChunk(TLocation* location, const TChunkDescriptor& descriptor)
    : Id_(descriptor.Id)
    , Location_(location)
    , Size_(descriptor.Size)
    , HasInfo(false)
{ }

Stroka TChunk::GetFileName()
{
    return Location_->GetChunkFileName(Id_);
}

TChunk::TAsyncGetInfoResult::TPtr TChunk::GetInfo()
{
    if (HasInfo) {
        return ToFuture(TGetInfoResult(Info));
    }

    // TODO:
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////

TStoredChunk::TStoredChunk(TLocation* location, const TChunkInfo& info)
    : TChunk(location, info)
{ }

TStoredChunk::TStoredChunk(TLocation* location, const TChunkDescriptor& descriptor)
    : TChunk(location, descriptor)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedChunk::TCachedChunk(TLocation* location, const TChunkInfo& info)
    : TChunk(location, info)
{ }

TCachedChunk::TCachedChunk(TLocation* location, const TChunkDescriptor& descriptor)
    : TChunk(location, descriptor)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
