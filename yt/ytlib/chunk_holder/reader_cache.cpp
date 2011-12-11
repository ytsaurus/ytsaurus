#include "stdafx.h"
#include "reader_cache.h"
#include "chunk.h"
#include "location.h"

#include "../misc/cache.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

class TReaderCache::TCachedReader
    : public TCacheValueBase<TChunkId, TCachedReader>
    , public TChunkFileReader
{
public:
    typedef TIntrusivePtr<TCachedReader> TPtr;

    TCachedReader(const TChunkId& chunkId, const Stroka& fileName)
        : TCacheValueBase<TChunkId, TCachedReader>(chunkId)
        , TChunkFileReader(fileName)
    { }

};

////////////////////////////////////////////////////////////////////////////////

class TReaderCache::TImpl
    : public TCapacityLimitedCache<TChunkId, TCachedReader>
{
public:
    typedef TIntrusivePtr<TReaderCache> TPtr;

    TImpl(const TChunkHolderConfig& config)
        : TCapacityLimitedCache<TChunkId, TCachedReader>(config.MaxCachedReaders)
    { }

    TGetReaderResult Get(const TChunk* chunk)
    {
        auto chunkId = chunk->GetId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            auto fileName = chunk->GetFileName();
            if (!isexist(~fileName)) {
                cookie.Cancel(TGetReaderResult(
                    EErrorCode::NoSuchChunk,
                    Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString())));
            }

            try {
                auto reader = New<TCachedReader>(chunkId, fileName);
                reader->Open();
                cookie.EndInsert(reader);
            } catch (...) {
                LOG_FATAL("Error opening chunk (ChunkId: %s)\n%s",
                    ~chunkId.ToString(),
                    ~CurrentExceptionMessage());
            }
        }

        return cookie.GetAsyncResult()->Get();
    }

    void Evict(const TChunk* chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

TReaderCache::TReaderCache(const TChunkHolderConfig& config)
    : Impl(New<TImpl>(config))
{ }

TReaderCache::TGetReaderResult TReaderCache::GetReader(const TChunk* chunk)
{
    return Impl->Get(chunk);
}

void TReaderCache::EvictReader(const TChunk* chunk)
{
    Impl->Evict(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
