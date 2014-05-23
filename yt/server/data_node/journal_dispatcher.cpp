#include "stdafx.h"
#include "journal_dispatcher.h"
#include "private.h"
#include "config.h"
#include "journal_chunk.h"
#include "location.h"

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

class TJournalDispatcher::TCachedJournal
    : public TCacheValueBase<TChunkId, TCachedJournal>
    , public IChangelog
{
public:
    TCachedJournal(const TChunkId& chunkId, IChangelogPtr changelog)
        : TCacheValueBase<TChunkId, TCachedJournal>(chunkId)
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
    : public TSizeLimitedCache<TChunkId, TCachedJournal>
{
public:
    explicit TImpl(
        TDataNodeConfigPtr config,
        const Stroka& threadName)
        : TSizeLimitedCache<TChunkId, TCachedJournal>(config->JournalCacheSize)
        , Config_(config)
        , ChangelogDispatcher_(New<TFileChangelogDispatcher>(threadName))
    { }

    IChangelogPtr GetJournal(TJournalChunkPtr chunk)
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
                    auto changelog = ChangelogDispatcher_->OpenChangelog(fileName, Config_->JournalChunks);
                    auto journal = New<TCachedJournal>(chunkId, changelog);
                    cookie.EndInsert(journal);
                } catch (const std::exception& ex) {
                    auto error = TError(
                        NChunkClient::EErrorCode::IOError,
                        "Error opening journal chunk %s",
                        ~ToString(chunkId))
                        << ex;
                    cookie.Cancel(error);
                    chunk->GetLocation()->Disable();
                    THROW_ERROR_EXCEPTION(error);
                }
            }

            LOG_DEBUG("Finished opening journal chunk (LocationId: %s, ChunkId: %s)",
                ~chunk->GetLocation()->GetId(),
                ~ToString(chunkId));
        }

        auto resultOrError = cookie.GetValue().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        return resultOrError.Value();
    }

    void EvictChangelog(TJournalChunkPtr chunk)
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

IChangelogPtr TJournalDispatcher::GetChangelog(TJournalChunkPtr chunk)
{
    return Impl_->GetJournal(chunk);
}

void TJournalDispatcher::EvictChangelog(TJournalChunkPtr chunk)
{
    Impl_->EvictChangelog(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
