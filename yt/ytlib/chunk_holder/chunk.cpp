#include "stdafx.h"
#include "chunk.h"
#include "location.h"
#include "reader_cache.h"
#include "chunk_holder_service_rpc.h"

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

Stroka TChunk::GetFileName() const
{
    return Location_->GetChunkFileName(Id_);
}

TChunk::TAsyncGetInfoResult::TPtr TChunk::GetInfo() const
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (HasInfo) {
            return ToFuture(TGetInfoResult(Info));
        }
    }

    TIntrusivePtr<const TChunk> chunk = this;
    auto invoker = Location_->GetInvoker();
    auto readerCache = Location_->GetReaderCache();
    return
        FromFunctor([=] () -> TGetInfoResult
            {
                auto result = readerCache->GetReader(~chunk);
                if (!result.IsOK()) {
                    return TError(result);
                }

                auto reader = result.Value();
                auto info = reader->GetChunkInfo();

                TGuard<TSpinLock> guard(SpinLock);
                Info = info;
                HasInfo = true;

                return info;
            })
        ->AsyncVia(invoker)
        ->Do();
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
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
{ }

TCachedChunk::TCachedChunk(TLocation* location, const TChunkDescriptor& descriptor)
    : TChunk(location, descriptor)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
{ }

void TCachedChunk::Aquire()
{
    // TODO: implement
}

void TCachedChunk::Release()
{
    // TODO: implement
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
