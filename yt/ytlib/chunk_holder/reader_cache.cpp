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
    , public TFileReader
{
public:
    typedef TIntrusivePtr<TCachedReader> TPtr;

    TCachedReader(const TChunkId& chunkId, const Stroka& fileName)
        : TCacheValueBase<TChunkId, TCachedReader>(chunkId)
        , TFileReader(fileName)
    { }

};

////////////////////////////////////////////////////////////////////////////////

class TReaderCache::TImpl
    : public TCapacityLimitedCache<TChunkId, TCachedReader>
{
public:
    typedef TIntrusivePtr<TReaderCache> TPtr;

    TImpl(const TChunkHolderConfig& config)
        : TCapacityLimitedCache<TChunkId, TCachedReader>(config.MaxCachedFiles)
    { }

    TCachedReader::TPtr Get(TChunk* chunk)
    {
        auto chunkId = chunk->GetId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            auto fileName = chunk->GetFileName();
            if (!isexist(~fileName)) {
                return NULL;
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

    void EvictReader(TChunk* chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

TReaderCache::TReaderCache(const TChunkHolderConfig& config)
    : Impl(New<TImpl>(config))
{ }

NChunkClient::TFileReader::TPtr TReaderCache::FindReader(TChunk* chunk)
{
    return Impl->Get(chunk);
}

void TReaderCache::EvictReader(TChunk* chunk)
{
    Impl->EvictReader(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
