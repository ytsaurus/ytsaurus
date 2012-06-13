#include "stdafx.h"
#include "reader_cache.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/misc/cache.h>

#include <util/folder/dirut.h>

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
    TCachedReader(const TChunkId& chunkId, const Stroka& fileName)
        : TCacheValueBase<TChunkId, TCachedReader>(chunkId)
        , TFileReader(fileName)
        , ChunkId(chunkId)
    { }

    virtual TChunkId GetChunkId() const
    {
        return ChunkId;
    }

private:
    TChunkId ChunkId;
};

////////////////////////////////////////////////////////////////////////////////

class TReaderCache::TImpl
    : public TSizeLimitedCache<TChunkId, TCachedReader>
{
public:
    TImpl(TChunkHolderConfigPtr config)
        : TSizeLimitedCache<TChunkId, TCachedReader>(config->MaxCachedReaders)
    { }

    TGetReaderResult Get(TChunkPtr chunk)
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
            } catch (const std::exception& ex) {
                LOG_FATAL("Error opening chunk (ChunkId: %s)\n%s",
                    ~chunkId.ToString(),
                    ex.what());
            }
        }

        return cookie.GetValue().Get();
    }

    void Evict(TChunkPtr chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

TReaderCache::TReaderCache(TChunkHolderConfigPtr config)
    : Impl(New<TImpl>(config))
{ }

TReaderCache::~TReaderCache()
{ }

TReaderCache::TGetReaderResult TReaderCache::GetReader(TChunkPtr chunk)
{
    return Impl->Get(chunk);
}

void TReaderCache::EvictReader(TChunkPtr chunk)
{
    Impl->Evict(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
