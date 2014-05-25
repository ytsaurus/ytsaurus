#include "stdafx.h"
#include "journal_dispatcher.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"
#include "session.h"

#include <core/misc/cache.h>

#include <server/hydra/changelog.h>
#include <server/hydra/file_changelog.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TCachedChangelog
    : public TCacheValueBase<TChunkId, TCachedChangelog>
    , public IChangelog
{
public:
    TCachedChangelog(const TChunkId& chunkId, IChangelogPtr changelog)
        : TCacheValueBase<TChunkId, TCachedChangelog>(chunkId)
        , Changelog_(changelog)
    { }

    virtual TSharedRef GetMeta() const override
    {
        return Changelog_->GetMeta();
    }

    virtual int GetRecordCount() const override
    {
        return Changelog_->GetRecordCount();
    }

    virtual i64 GetDataSize() const override
    {
        return Changelog_->GetDataSize();
    }

    virtual bool IsSealed() const override
    {
        return Changelog_->IsSealed();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        return Changelog_->Append(data);
    }

    virtual TFuture<void> Flush() override
    {
        return Changelog_->Flush();
    }

    virtual void Close() override
    {
        Changelog_->Close();
    }

    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        return Changelog_->Read(
            firstRecordId,
            maxRecords,
            maxBytes);
    }

    virtual TFuture<void> Seal(int recordCount) override
    {
        return Changelog_->Seal(recordCount);
    }

    virtual void Unseal() override
    {
        Changelog_->Unseal();
    }

private:
    IChangelogPtr Changelog_;

};

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TImpl
    : public TSizeLimitedCache<TChunkId, TCachedChangelog>
{
public:
    explicit TImpl(
        TDataNodeConfigPtr config,
        const Stroka& threadName)
        : TSizeLimitedCache<TChunkId, TCachedChangelog>(config->JournalDispatcher->ReaderCacheSize)
        , Config_(config)
        , ChangelogDispatcher_(New<TFileChangelogDispatcher>(threadName))
    { }

    bool AcceptsChunks() const
    {
        return Config_->JournalDispatcher->Multiplexed != nullptr;
    }

    IChangelogPtr GetChangelog(IChunkPtr chunk)
    {
        YCHECK(chunk->IsReadLockAcquired());

        auto location = chunk->GetLocation();
        auto& Profiler = location->Profiler();

        auto chunkId = chunk->GetId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            auto fileName = chunk->GetFileName();
            LOG_DEBUG("Started opening journal chunk (LocationId: %s, ChunkId: %s)",
                ~location->GetId(),
                ~ToString(chunkId));

            PROFILE_TIMING ("/journal_chunk_open_time") {
                try {
                    auto changelog = ChangelogDispatcher_->OpenChangelog(
                        fileName,
                        Config_->JournalDispatcher->Split);
                    auto cachedChangelog = New<TCachedChangelog>(chunkId, changelog);
                    cookie.EndInsert(cachedChangelog);
                } catch (const std::exception& ex) {
                    auto error = TError(
                        NChunkClient::EErrorCode::IOError,
                        "Error opening journal chunk %s",
                        ~ToString(chunkId))
                        << ex;
                    cookie.Cancel(error);
                    location->Disable();
                    THROW_ERROR_EXCEPTION(error);
                }
            }

            LOG_DEBUG("Finished opening journal chunk (LocationId: %s, ChunkId: %s)",
                ~location->GetId(),
                ~ToString(chunkId));
        }

        auto resultOrError = cookie.GetValue().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        return resultOrError.Value();
    }

    IChangelogPtr CreateChangelog(ISessionPtr session)
    {
        if (!AcceptsChunks()) {
            THROW_ERROR_EXCEPTION("No new journal chunks are accepted");
        }

        auto location = session->GetLocation();
        auto& Profiler = location->Profiler();

        auto chunkId = session->GetChunkId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            auto fileName = location->GetChunkFileName(chunkId);
            LOG_DEBUG("Started creating journal chunk (LocationId: %s, ChunkId: %s)",
                ~location->GetId(),
                ~ToString(chunkId));

            PROFILE_TIMING ("/journal_chunk_create_time") {
                try {
                    auto changelog = ChangelogDispatcher_->CreateChangelog(
                        fileName,
                        TSharedRef(), // TODO(babenko): journal meta
                        Config_->JournalDispatcher->Split);
                    auto cachedChangelog = New<TCachedChangelog>(chunkId, changelog);
                    cookie.EndInsert(cachedChangelog);
                } catch (const std::exception& ex) {
                    auto error = TError(
                        NChunkClient::EErrorCode::IOError,
                        "Error creating journal chunk %s",
                        ~ToString(chunkId))
                        << ex;
                    cookie.Cancel(error);
                    location->Disable();
                    THROW_ERROR_EXCEPTION(error);
                }
            }

            LOG_DEBUG("Finished creating journal chunk (LocationId: %s, ChunkId: %s)",
                ~location->GetId(),
                ~ToString(chunkId));
        }

        auto resultOrError = cookie.GetValue().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        return resultOrError.Value();
    }

    void EvictChangelog(IChunkPtr chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }

private:
    TDataNodeConfigPtr Config_;
    TFileChangelogDispatcherPtr ChangelogDispatcher_;

};

////////////////////////////////////////////////////////////////////////////////

TJournalDispatcher::TJournalDispatcher(
    TDataNodeConfigPtr config,
    const Stroka& threadName)
    : Impl_(New<TImpl>(config, threadName))
{ }

TJournalDispatcher::~TJournalDispatcher()
{ }

bool TJournalDispatcher::AcceptsChunks() const
{
    return Impl_->AcceptsChunks();
}

IChangelogPtr TJournalDispatcher::GetChangelog(IChunkPtr chunk)
{
    return Impl_->GetChangelog(chunk);
}

IChangelogPtr TJournalDispatcher::CreateChangelog(ISessionPtr session)
{
    return Impl_->CreateChangelog(session);
}

void TJournalDispatcher::EvictChangelog(IChunkPtr chunk)
{
    Impl_->EvictChangelog(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
