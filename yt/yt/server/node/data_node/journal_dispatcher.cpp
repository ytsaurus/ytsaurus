#include "journal_dispatcher.h"

#include "private.h"
#include "chunk.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_manager.h"
#include "location.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra_common/file_changelog.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NDataNode {

using namespace NHydra;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

DECLARE_REFCOUNTED_CLASS(TCachedChangelog)
DECLARE_REFCOUNTED_CLASS(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

struct TCachedChangelogKey
{
    TStoreLocationPtr Location;
    TChunkId ChunkId;

    // Hasher.
    operator size_t() const
    {
        size_t result = 0;
        HashCombine(result, Location);
        HashCombine(result, ChunkId);
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

class TJournalDispatcher
    : public TAsyncSlruCacheBase<TCachedChangelogKey, TCachedChangelog>
    , public IJournalDispatcher
{
public:
    explicit TJournalDispatcher(
        TDataNodeConfigPtr dataNodeConfig,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager)
        : TAsyncSlruCacheBase(
            dataNodeConfig->ChangelogReaderCache,
            DataNodeProfiler.WithPrefix("/changelog_reader_cache"))
    {
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TJournalDispatcher::OnDynamicConfigChanged, MakeWeak(this)));
    }

    TFuture<IFileChangelogPtr> OpenJournal(
        const TStoreLocationPtr& location,
        TChunkId chunkId) override;

    TFuture<IFileChangelogPtr> CreateJournal(
        const TStoreLocationPtr& location,
        TChunkId chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor) override;

    TFuture<void> RemoveJournal(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing) override;

    TFuture<bool> IsJournalSealed(
        const TStoreLocationPtr& location,
        TChunkId chunkId) override;

    TFuture<void> SealJournal(TJournalChunkPtr chunk) override;

private:
    friend class TCachedChangelog;

    IFileChangelogPtr OnChangelogOpenedOrCreated(
        TStoreLocationPtr location,
        TChunkId chunkId,
        bool enableMultiplexing,
        TInsertCookie cookie,
        const TErrorOr<IFileChangelogPtr>& changelogOrError);

    void OnAdded(const TCachedChangelogPtr& changelog) override;
    void OnRemoved(const TCachedChangelogPtr& changelog) override;

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->DataNode;
        TAsyncSlruCacheBase::Reconfigure(config->ChangelogReaderCache);
    }
};

DEFINE_REFCOUNTED_TYPE(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

class TCachedChangelog
    : public TAsyncCacheValueBase<TCachedChangelogKey, TCachedChangelog>
    , public IFileChangelog
{
public:
    TCachedChangelog(
        TJournalDispatcherPtr owner,
        TStoreLocationPtr location,
        TChunkId chunkId,
        IFileChangelogPtr underlyingChangelog,
        bool enableMultiplexing)
        : TAsyncCacheValueBase({location, chunkId})
        , Owner_(std::move(owner))
        , Location_(std::move(location))
        , ChunkId_(chunkId)
        , EnableMultiplexing_(enableMultiplexing)
        , UnderlyingChangelog_(std::move(underlyingChangelog))
    { }

    ~TCachedChangelog()
    {
        YT_LOG_DEBUG("Cached changelog destroyed (LocationId: %v, ChunkId: %v)",
            Location_->GetId(),
            ChunkId_);
    }

    int GetId() const override
    {
        return UnderlyingChangelog_->GetId();
    }

    const NHydra::NProto::TChangelogMeta& GetMeta() const override
    {
        return UnderlyingChangelog_->GetMeta();
    }

    int GetRecordCount() const override
    {
        return UnderlyingChangelog_->GetRecordCount();
    }

    i64 GetDataSize() const override
    {
        return UnderlyingChangelog_->GetDataSize();
    }

    i64 EstimateChangelogSize(i64 payloadSize) const override
    {
        auto result = UnderlyingChangelog_->EstimateChangelogSize(payloadSize);
        if (EnableMultiplexing_) {
            const auto& journalManager = Location_->GetJournalManager();
            result += journalManager->EstimateMultiplexedChangelogSize(payloadSize);
        }

        return result;
    }

    TFuture<void> Append(TRange<TSharedRef> records) override
    {
        int firstRecordId = UnderlyingChangelog_->GetRecordCount();
        auto flushResult = UnderlyingChangelog_->Append(records);
        if (!EnableMultiplexing_) {
            return flushResult.ToUncancelable();
        }

        const auto& journalManager = Location_->GetJournalManager();
        auto multiplexedFlushResult = journalManager->AppendMultiplexedRecords(
            ChunkId_,
            firstRecordId,
            records,
            flushResult);

        return multiplexedFlushResult
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (bool skipped) {
                // We provide the most strong semantic possible.
                // Concurrent Append()-s are permitted. Successful completion of last append,
                // guarantees that all previous records are committed to disk.
                if (skipped) {
                    ++RejectedMultiplexedAppends_;
                    flushResult.Apply(BIND([=, this, this_ = MakeStrong(this)] {
                        YT_VERIFY(--RejectedMultiplexedAppends_ >= 0);
                    }));
                }

                return RejectedMultiplexedAppends_ ? flushResult : VoidFuture;
            }))
            .ToUncancelable();
    }

    TFuture<void> Flush() override
    {
        return UnderlyingChangelog_->Flush()
            .ToUncancelable();
    }

    TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        return UnderlyingChangelog_->Read(firstRecordId, maxRecords, maxBytes);
    }

    NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor) override
    {
        return UnderlyingChangelog_->MakeChunkFragmentReadRequest(fragmentDescriptor);
    }

    TFuture<void> Truncate(int recordCount) override
    {
        return UnderlyingChangelog_->Truncate(recordCount)
            .ToUncancelable();
    }

    TFuture<void> Close() override
    {
        return UnderlyingChangelog_->Close().Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            Owner_->TryRemoveValue(this, /*forbidResurrection*/ true);
            return error;
        })).ToUncancelable();
    }

    TFuture<void> Finish() override
    {
        return UnderlyingChangelog_->Finish()
            .ToUncancelable();
    }

private:
    const TJournalDispatcherPtr Owner_;
    const TStoreLocationPtr Location_;
    const TChunkId ChunkId_;
    const bool EnableMultiplexing_;
    const IFileChangelogPtr UnderlyingChangelog_;

    std::atomic<int> RejectedMultiplexedAppends_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TCachedChangelog)

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileChangelogPtr> TJournalDispatcher::OpenJournal(
    const TStoreLocationPtr& location,
    TChunkId chunkId)
{
    auto cookie = BeginInsert({location, chunkId});
    if (!cookie.IsActive()) {
        return cookie.GetValue().As<IFileChangelogPtr>();
    }

    const auto& journalManager = location->GetJournalManager();
    return journalManager->OpenChangelog(chunkId).Apply(BIND(
        &TJournalDispatcher::OnChangelogOpenedOrCreated,
        MakeStrong(this),
        location,
        chunkId,
        false,
        Passed(std::move(cookie))));
}

IFileChangelogPtr TJournalDispatcher::OnChangelogOpenedOrCreated(
    TStoreLocationPtr location,
    TChunkId chunkId,
    bool enableMultiplexing,
    TInsertCookie cookie,
    const TErrorOr<IFileChangelogPtr>& changelogOrError)
{
    if (!changelogOrError.IsOK()) {
        cookie.Cancel(changelogOrError);
        THROW_ERROR changelogOrError;
    }

    const auto& changelog = changelogOrError.Value();
    auto cachedChangelog = New<TCachedChangelog>(
        this,
        location,
        chunkId,
        changelog,
        enableMultiplexing);
    cookie.EndInsert(cachedChangelog);
    return cachedChangelog;
}

TFuture<IFileChangelogPtr> TJournalDispatcher::CreateJournal(
    const TStoreLocationPtr& location,
    TChunkId chunkId,
    bool enableMultiplexing,
    const TWorkloadDescriptor& workloadDescriptor)
{
    try {
        auto cookie = BeginInsert({location, chunkId});
        if (!cookie.IsActive()) {
            THROW_ERROR_EXCEPTION("Journal chunk %v is still busy",
                chunkId);
        }

        const auto& journalManager = location->GetJournalManager();
        auto changelogFuture = journalManager->CreateChangelog(
            chunkId,
            enableMultiplexing,
            workloadDescriptor);
        return changelogFuture.Apply(BIND(
            &TJournalDispatcher::OnChangelogOpenedOrCreated,
            MakeStrong(this),
            location,
            chunkId,
            enableMultiplexing,
            Passed(std::move(cookie))));
    } catch (const std::exception& ex) {
        return MakeFuture<IFileChangelogPtr>(ex);
    }
}

TFuture<void> TJournalDispatcher::RemoveJournal(
    const TJournalChunkPtr& chunk,
    bool enableMultiplexing)
{
    const auto& location = chunk->GetStoreLocation();

    TAsyncSlruCacheBase::TryRemove({location, chunk->GetId()});

    const auto& journalManager = location->GetJournalManager();
    return journalManager->RemoveChangelog(chunk, enableMultiplexing);
}

TFuture<bool> TJournalDispatcher::IsJournalSealed(
    const TStoreLocationPtr& location,
    TChunkId chunkId)
{
    const auto& journalManager = location->GetJournalManager();
    return journalManager->IsChangelogSealed(chunkId);
}

TFuture<void> TJournalDispatcher::SealJournal(TJournalChunkPtr chunk)
{
    const auto& location = chunk->GetStoreLocation();
    const auto& journalManager = location->GetJournalManager();
    return journalManager->SealChangelog(chunk);
}

void TJournalDispatcher::OnAdded(const TCachedChangelogPtr& changelog)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TAsyncSlruCacheBase::OnAdded(changelog);

    auto key = changelog->GetKey();
    YT_LOG_DEBUG("Changelog added to cache (LocationId: %v, ChunkId: %v)",
        key.Location->GetId(),
        key.ChunkId);
}

void TJournalDispatcher::OnRemoved(const TCachedChangelogPtr& changelog)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TAsyncSlruCacheBase::OnRemoved(changelog);

    auto key = changelog->GetKey();
    YT_LOG_DEBUG("Changelog removed from cache (LocationId: %v, ChunkId: %v)",
        key.Location->GetId(),
        key.ChunkId);
}

////////////////////////////////////////////////////////////////////////////////

IJournalDispatcherPtr CreateJournalDispatcher(
    TDataNodeConfigPtr dataNodeConfig,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager)
{
    return New<TJournalDispatcher>(
        dataNodeConfig,
        dynamicConfigManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
