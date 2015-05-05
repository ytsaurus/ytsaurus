#include "stdafx.h"
#include "journal_dispatcher.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "journal_chunk.h"
#include "location.h"
#include "journal_manager.h"
#include "chunk_store.h"

#include <core/misc/async_cache.h>

#include <server/hydra/changelog.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NHydra;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TImpl
    : public TAsyncSlruCacheBase<TChunkId, TCachedChangelog>
{
public:
    TImpl(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : TAsyncSlruCacheBase<TChunkId, TCachedChangelog>(
            config->ChangelogReaderCache,
            NProfiling::TProfiler(DataNodeProfiler.GetPathPrefix() + "/changelog_cache"))
        , Config_(config)
        , Bootstrap_(bootstrap)
    { }

    TFuture<IChangelogPtr> OpenChangelog(
        TLocationPtr location,
        const TChunkId& chunkId);

    TFuture<IChangelogPtr> CreateChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    TFuture<void> RemoveChangelog(
        TJournalChunkPtr chunk,
        bool enableMultiplexing);

private:
    friend class TCachedChangelog;

    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    IChangelogPtr OnChangelogOpenedOrCreated(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing,
        TInsertCookie cookie,
        const TErrorOr<IChangelogPtr>& changelogOrError);

    virtual void OnAdded(TCachedChangelog* changelog) override;
    virtual void OnRemoved(TCachedChangelog* changelog) override;

};

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TCachedChangelog
    : public TAsyncCacheValueBase<TChunkId, TCachedChangelog>
    , public IChangelog
{
public:
    TCachedChangelog(
        TImplPtr owner,
        TLocationPtr location,
        const TChunkId& chunkId,
        IChangelogPtr underlyingChangelog,
        bool enableMultiplexing)
        : TAsyncCacheValueBase<TChunkId, TCachedChangelog>(chunkId)
        , Owner_(owner)
        , Location_(location)
        , EnableMultiplexing_(enableMultiplexing)
        , UnderlyingChangelog_(underlyingChangelog)
    { }

    ~TCachedChangelog()
    {
        LOG_DEBUG("Cached changelog destroyed (ChunkId: %v)",
            GetKey());
    }

    virtual const TChangelogMeta& GetMeta() const override
    {
        return UnderlyingChangelog_->GetMeta();
    }

    virtual int GetRecordCount() const override
    {
        return UnderlyingChangelog_->GetRecordCount();
    }

    virtual i64 GetDataSize() const override
    {
        return UnderlyingChangelog_->GetDataSize();
    }

    virtual bool IsSealed() const override
    {
        return UnderlyingChangelog_->IsSealed();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        if (!EnableMultiplexing_) {
            return UnderlyingChangelog_->Append(data);
        }

        int recordId = UnderlyingChangelog_->GetRecordCount();
        auto flushResult = UnderlyingChangelog_->Append(data);
        auto journalManager = Location_->GetJournalManager();
        return journalManager->AppendMultiplexedRecord(
            GetKey(),
            recordId,
            data,
            flushResult);
    }

    virtual TFuture<void> Flush() override
    {
        return UnderlyingChangelog_->Flush();
    }

    virtual TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        return UnderlyingChangelog_->Read(firstRecordId, maxRecords, maxBytes);
    }

    virtual TFuture<void> Seal(int recordCount) override
    {
        return UnderlyingChangelog_->Seal(recordCount);
    }

    virtual TFuture<void> Unseal() override
    {
        return UnderlyingChangelog_->Unseal();
    }

    virtual TFuture<void> Close() override
    {
        Owner_->TryRemove(this);
        return UnderlyingChangelog_->Close();
    }

private:
    const TImplPtr Owner_;
    const TLocationPtr Location_;
    const bool EnableMultiplexing_;
    const IChangelogPtr UnderlyingChangelog_;

};

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogPtr> TJournalDispatcher::TImpl::OpenChangelog(
    TLocationPtr location,
    const TChunkId& chunkId)
{
    TInsertCookie cookie(chunkId);
    bool inserted = BeginInsert(&cookie);
    auto result = cookie.GetValue();

    if (!inserted) {
        return result.As<IChangelogPtr>();
    }

    auto journalManager = location->GetJournalManager();
    return journalManager->OpenChangelog(chunkId).Apply(BIND(
        &TImpl::OnChangelogOpenedOrCreated,
        MakeStrong(this),
        location,
        chunkId,
        false,
        Passed(std::move(cookie))));
}

IChangelogPtr TJournalDispatcher::TImpl::OnChangelogOpenedOrCreated(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing,
    TInsertCookie cookie,
    const TErrorOr<IChangelogPtr>& changelogOrError)
{
    if (!changelogOrError.IsOK()) {
        cookie.Cancel(changelogOrError);
        THROW_ERROR changelogOrError;
    }

    auto cachedChangelog = New<TCachedChangelog>(
        this,
        location,
        chunkId,
        changelogOrError.Value(),
        enableMultiplexing);
    cookie.EndInsert(cachedChangelog);
    return cachedChangelog;
}

TFuture<IChangelogPtr> TJournalDispatcher::TImpl::CreateChangelog(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    try {
        TInsertCookie cookie(chunkId);
        if (!BeginInsert(&cookie)) {
            THROW_ERROR_EXCEPTION("Journal chunk %v is still busy",
                chunkId);
        }

        auto journalManager = location->GetJournalManager();
        return journalManager->CreateChangelog(chunkId, enableMultiplexing).Apply(BIND(
            &TImpl::OnChangelogOpenedOrCreated,
            MakeStrong(this),
            location,
            chunkId,
            enableMultiplexing,
            Passed(std::move(cookie))));
    } catch (const std::exception& ex) {
        return MakeFuture<IChangelogPtr>(ex);
    }
}

TFuture<void> TJournalDispatcher::TImpl::RemoveChangelog(
    TJournalChunkPtr chunk,
    bool enableMultiplexing)
{
    TAsyncSlruCacheBase::TryRemove(chunk->GetId());

    auto location = chunk->GetLocation();
    auto journalManager = location->GetJournalManager();
    return journalManager->RemoveChangelog(chunk, enableMultiplexing);
}

void TJournalDispatcher::TImpl::OnAdded(TCachedChangelog* changelog)
{
    LOG_TRACE("Journal chunk added to cache (ChunkId: %v)",
        changelog->GetKey());
}

void TJournalDispatcher::TImpl::OnRemoved(TCachedChangelog* changelog)
{
    LOG_TRACE("Journal chunk evicted from cache (ChunkId: %v)",
        changelog->GetKey());
}

////////////////////////////////////////////////////////////////////////////////

TJournalDispatcher::TJournalDispatcher(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TJournalDispatcher::~TJournalDispatcher()
{ }

TFuture<IChangelogPtr> TJournalDispatcher::OpenChangelog(
    TLocationPtr location,
    const TChunkId& chunkId)
{
    return Impl_->OpenChangelog(location, chunkId);
}

TFuture<IChangelogPtr> TJournalDispatcher::CreateChangelog(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    return Impl_->CreateChangelog(location, chunkId, enableMultiplexing);
}

TFuture<void> TJournalDispatcher::RemoveChangelog(
    TJournalChunkPtr chunk,
    bool enableMultiplexing)
{
    return Impl_->RemoveChangelog(chunk, enableMultiplexing);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
