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

struct TJournalDispatcher::TCachedChangelogKey
{
    TStoreLocationPtr Location;
    TChunkId ChunkId;

    // Hasher.
    operator size_t() const
    {
        size_t result = 0;
        result = HashCombine(result, Location);
        result = HashCombine(result, ChunkId);
        return result;
    }

    // Comparer.
    bool operator == (const TCachedChangelogKey& other) const
    {
        return
            Location == other.Location &&
            ChunkId == other.ChunkId;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TImpl
    : public TAsyncSlruCacheBase<TCachedChangelogKey, TCachedChangelog>
{
public:
    explicit TImpl(TDataNodeConfigPtr config)
        : TAsyncSlruCacheBase(
            config->ChangelogReaderCache,
            NProfiling::TProfiler(DataNodeProfiler.GetPathPrefix() + "/changelog_cache"))
        , Config_(config)
    { }

    TFuture<IChangelogPtr> OpenChangelog(
        TStoreLocationPtr location,
        const TChunkId& chunkId);

    TFuture<IChangelogPtr> CreateChangelog(
        TStoreLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    TFuture<void> RemoveChangelog(
        TJournalChunkPtr chunk,
        bool enableMultiplexing);

    TFuture<bool> IsChangelogSealed(
        TStoreLocationPtr location,
        const TChunkId& chunkId);

    TFuture<void> SealChangelog(TJournalChunkPtr chunk);

private:
    friend class TCachedChangelog;

    const TDataNodeConfigPtr Config_;


    IChangelogPtr OnChangelogOpenedOrCreated(
        TStoreLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing,
        TInsertCookie cookie,
        const TErrorOr<IChangelogPtr>& changelogOrError);

    virtual void OnAdded(TCachedChangelog* changelog) override;
    virtual void OnRemoved(TCachedChangelog* changelog) override;

};

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TCachedChangelog
    : public TAsyncCacheValueBase<TCachedChangelogKey, TCachedChangelog>
    , public IChangelog
{
public:
    TCachedChangelog(
        TImplPtr owner,
        TStoreLocationPtr location,
        const TChunkId& chunkId,
        IChangelogPtr underlyingChangelog,
        bool enableMultiplexing)
        : TAsyncCacheValueBase({location, chunkId})
        , Owner_(owner)
        , Location_(location)
        , ChunkId_(chunkId)
        , EnableMultiplexing_(enableMultiplexing)
        , UnderlyingChangelog_(underlyingChangelog)
    { }

    ~TCachedChangelog()
    {
        LOG_DEBUG("Cached changelog destroyed (LocationId: %v, ChunkId: %v)",
            Location_->GetId(),
            ChunkId_);
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

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        if (!EnableMultiplexing_) {
            return UnderlyingChangelog_->Append(data);
        }

        int recordId = UnderlyingChangelog_->GetRecordCount();
        auto flushResult = UnderlyingChangelog_->Append(data);
        auto journalManager = Location_->GetJournalManager();
        return journalManager->AppendMultiplexedRecord(
            ChunkId_,
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

    virtual TFuture<void> Truncate(int recordCount) override
    {
        return UnderlyingChangelog_->Truncate(recordCount);
    }

    virtual TFuture<void> Close() override
    {
        Owner_->TryRemove(this);
        return UnderlyingChangelog_->Close();
    }

private:
    const TImplPtr Owner_;
    const TStoreLocationPtr Location_;
    const TChunkId ChunkId_;
    const bool EnableMultiplexing_;
    const IChangelogPtr UnderlyingChangelog_;

};

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogPtr> TJournalDispatcher::TImpl::OpenChangelog(
    TStoreLocationPtr location,
    const TChunkId& chunkId)
{
    auto cookie = BeginInsert({location, chunkId});
    if (!cookie.IsActive()) {
        return cookie.GetValue().As<IChangelogPtr>();
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
    TStoreLocationPtr location,
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
    TStoreLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    try {
        auto cookie = BeginInsert({location, chunkId});
        if (!cookie.IsActive()) {
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
    auto location = chunk->GetStoreLocation();

    TAsyncSlruCacheBase::TryRemove({location, chunk->GetId()});

    auto journalManager = location->GetJournalManager();
    return journalManager->RemoveChangelog(chunk, enableMultiplexing);
}

TFuture<bool> TJournalDispatcher::TImpl::IsChangelogSealed(
    TStoreLocationPtr location,
    const TChunkId& chunkId)
{
    auto journalManager = location->GetJournalManager();
    return journalManager->IsChangelogSealed(chunkId);
}

TFuture<void> TJournalDispatcher::TImpl::SealChangelog(TJournalChunkPtr chunk)
{
    auto location = chunk->GetStoreLocation();
    auto journalManager = location->GetJournalManager();
    return journalManager->SealChangelog(chunk);
}

void TJournalDispatcher::TImpl::OnAdded(TCachedChangelog* changelog)
{
    auto key = changelog->GetKey();
    LOG_TRACE("Journal chunk added to cache (LocationId: %v, ChunkId: %v)",
        key.Location->GetId(),
        key.ChunkId);
}

void TJournalDispatcher::TImpl::OnRemoved(TCachedChangelog* changelog)
{
    auto key = changelog->GetKey();
    LOG_TRACE("Journal chunk removed from cache (LocationId: %v, ChunkId: %v)",
        key.Location->GetId(),
        key.ChunkId);
}

////////////////////////////////////////////////////////////////////////////////

TJournalDispatcher::TJournalDispatcher(TDataNodeConfigPtr config)
    : Impl_(New<TImpl>(config))
{ }

TJournalDispatcher::~TJournalDispatcher()
{ }

TFuture<IChangelogPtr> TJournalDispatcher::OpenChangelog(
    TStoreLocationPtr location,
    const TChunkId& chunkId)
{
    return Impl_->OpenChangelog(location, chunkId);
}

TFuture<IChangelogPtr> TJournalDispatcher::CreateChangelog(
    TStoreLocationPtr location,
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

TFuture<bool> TJournalDispatcher::IsChangelogSealed(
    TStoreLocationPtr location,
    const TChunkId& chunkId)
{
    return Impl_->IsChangelogSealed(location, chunkId);
}

TFuture<void> TJournalDispatcher::SealChangelog(TJournalChunkPtr chunk)
{
    return Impl_->SealChangelog(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
