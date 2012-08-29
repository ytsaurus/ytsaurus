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

static NLog::TLogger& Logger = DataNodeLogger;
static NProfiling::TProfiler& Profiler = DataNodeProfiler;

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
    explicit TImpl(TDataNodeConfigPtr config)
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

            LOG_DEBUG("Started opening chunk reader (LocationId: %s, ChunkId: %s)",
                ~chunk->GetLocation()->GetId(),
                ~chunkId.ToString());

            PROFILE_TIMING ("/chunk_io/chunk_reader_open_time") {
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

            LOG_DEBUG("Finished opening chunk reader (LocationId: %s, ChunkId: %s)",
                ~chunk->GetLocation()->GetId(),
                ~chunkId.ToString());
        }

        return cookie.GetValue().Get();
    }

    void Evict(TChunk* chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

TReaderCache::TReaderCache(TDataNodeConfigPtr config)
    : Impl(New<TImpl>(config))
{ }

TReaderCache::~TReaderCache()
{ }

TReaderCache::TGetReaderResult TReaderCache::GetReader(TChunkPtr chunk)
{
    return Impl->Get(chunk);
}

void TReaderCache::EvictReader(TChunk* chunk)
{
    Impl->Evict(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
