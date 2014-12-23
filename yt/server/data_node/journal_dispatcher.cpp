#include "stdafx.h"
#include "journal_dispatcher.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"
#include "session.h"
#include "chunk_store.h"
#include "journal_chunk.h"

#include <core/misc/async_cache.h>
#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/periodic_executor.h>

#include <server/hydra/changelog.h>
#include <server/hydra/file_changelog_dispatcher.h>
#include <server/hydra/lazy_changelog.h>
#include <server/hydra/sync_file_changelog.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NHydra;
using namespace NHydra::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

static const auto CleanExtension = Stroka("clean");
static const auto CleanupPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EMultiplexedRecordType,
    (Create) // create chunk
    (Append) // append record to chunk
    (Remove) // remove chunk
);

#pragma pack(push, 4)

struct TMultiplexedRecordHeader
{
    //! Type of the record.
    EMultiplexedRecordType Type;

    // Record id within the chunk (for |Append| only).
    i32 RecordId;

    //! Id of chunk this record is about.
    TChunkId ChunkId;

    //! For |Append| type the data follows.
};

static_assert(sizeof(TMultiplexedRecordHeader) == 24, "Binary size of TMultiplexedRecordHeader has changed.");

#pragma pack(pop)

struct TMultiplexedRecord
{
    TMultiplexedRecordHeader Header;
    TSharedRef Data;
};

} // namespace NDataNode
} // namespace NYT

DECLARE_PODTYPE(NYT::NDataNode::TMultiplexedRecordHeader);

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka ChopExtension(Stroka* fileName)
{
    auto extension = NFS::GetFileExtension(*fileName);
    *fileName = NFS::GetFileNameWithoutExtension(*fileName);
    return extension;
}

int ParseChangelogId(const Stroka& str, const Stroka& fileName)
{
    try {
        return FromString<int>(str);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing multiplexed changelog id %Qv",
            fileName);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TImpl
    : public TAsyncSlruCacheBase<TChunkId, TCachedChangelog>
{
public:
    explicit TImpl(
        NCellNode::TBootstrap* bootstrap,
        TDataNodeConfigPtr config)
        : TAsyncSlruCacheBase<TChunkId, TCachedChangelog>(config->ChangelogReaderCache)
        , Bootstrap_(bootstrap)
        , Config_(config)
        , ChangelogDispatcher_(New<TFileChangelogDispatcher>("JournalFlush"))
    { }

    void Initialize();

    bool AcceptsChunks() const
    {
        return Config_->MultiplexedChangelog != nullptr;
    }

    TFuture<TErrorOr<IChangelogPtr>> OpenChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    TFuture<TErrorOr<IChangelogPtr>> CreateChangelog(
        TJournalChunkPtr chunk,
        bool enableMultiplexing);

    TAsyncError RemoveChangelog(TJournalChunkPtr chunk);

private:
    friend class TCachedChangelog;
    friend class TMultiplexedReplay;

    NCellNode::TBootstrap* Bootstrap_;
    TDataNodeConfigPtr Config_;

    TFileChangelogDispatcherPtr ChangelogDispatcher_;

    //! Protects a section of members.
    TSpinLock SpinLock_;

    //! The current multiplexed changelog.
    IChangelogPtr MultiplexedChangelog_;

    //! The id of #MultiplexedChangelog.
    int MultiplexedChangelogId_;

    //! The set of changelogs whose records were added into the current multiplexed changelog.
    //! Safeguards marking multiplexed changelogs as clean.
    //! NB: Changelogs are removed by chunk id, hence the use of |yhash_map| rather than |yhash_set|.
    yhash_map<TChunkId, TCachedChangelogPtr> ActiveChangelogs_;

    TPeriodicExecutorPtr CleanupExecutor_;


    void AddChangelogToActive(const TChunkId& chunkId, TCachedChangelogPtr changelog)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (ActiveChangelogs_.insert(std::make_pair(chunkId, changelog)).second) {
            LOG_DEBUG("Changelog is added to active set (ChunkId: %v)",
                chunkId);
        }
    }

    void RemoveChangelogFromActive(const TChunkId& chunkId)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (ActiveChangelogs_.erase(chunkId) == 1) {
            LOG_DEBUG("Changelog is removed from active set (ChunkId: %v)",
                chunkId);
        }
    }

    void ClearActiveChangelogs()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        for (const auto& pair : ActiveChangelogs_) {
            LOG_DEBUG("Changelog is removed from active set (ChunkId: %v)",
                pair.first);
        }
        ActiveChangelogs_.clear();
    }


    IChangelogPtr DoCreateChangelog(IChunkPtr chunk)
    {
        const auto& chunkId = chunk->GetId();
        auto location = chunk->GetLocation();

        LOG_DEBUG("Started creating journal chunk (LocationId: %v, ChunkId: %v)",
            location->GetId(),
            chunkId);

        IChangelogPtr changelog;

        auto& Profiler = location->Profiler();
        PROFILE_TIMING("/journal_chunk_create_time") {
            try {
                auto fileName = location->GetChunkFileName(chunkId);
                changelog = ChangelogDispatcher_->CreateChangelog(
                    fileName,
                    TChangelogMeta(),
                    Config_->SplitChangelog);
            } catch (const std::exception& ex) {
                auto error = TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error creating journal chunk %v",
                    chunkId) << ex;
                location->Disable(error);
                THROW_ERROR error;
            }
        }

        LOG_DEBUG("Finished creating journal chunk (LocationId: %v, ChunkId: %v)",
            location->GetId(),
            chunkId);

        return changelog;
    }

    void DoOpenChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing,
        TInsertCookie cookie)
    {
        auto fileName = location->GetChunkFileName(chunkId);
        LOG_DEBUG("Started opening journal chunk (LocationId: %v, ChunkId: %v)",
            location->GetId(),
            chunkId);

        auto& Profiler = location->Profiler();
        PROFILE_TIMING("/journal_chunk_open_time") {
            try {
                auto changelog = ChangelogDispatcher_->OpenChangelog(
                    fileName,
                    Config_->SplitChangelog);
                auto cachedChangelog = New<TCachedChangelog>(
                    this,
                    chunkId,
                    changelog,
                    enableMultiplexing);
                cookie.EndInsert(cachedChangelog);
            } catch (const std::exception& ex) {
                auto error = TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error opening journal chunk %v",
                    chunkId)
                    << ex;
                cookie.Cancel(error);
                location->Disable(error);
            }
        }

        LOG_DEBUG("Finished opening journal chunk (LocationId: %v, ChunkId: %v)",
            location->GetId(),
            chunkId);
    }

    void DoRemoveChangelog(TJournalChunkPtr chunk)
    {
        const auto& chunkId = chunk->GetId();

        TAsyncSlruCacheBase::TryRemove(chunkId);

        auto location = chunk->GetLocation();
        auto& Profiler = location->Profiler();
        PROFILE_TIMING("/journal_chunk_remove_time") {
            try {
                chunk->SyncRemove();
            } catch (const std::exception& ex) {
                auto error = TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error removing journal chunk %v",
                    chunkId) << ex;
                location->Disable(error);
                THROW_ERROR error;
            }
        }
    }


    TAsyncError AppendMultiplexedRecord(const TMultiplexedRecord& record, TCachedChangelogPtr changelog);

    TAsyncError DoAppendMultiplexedRecord(const TMultiplexedRecord& record)
    {
        auto multiplexedData = TSharedRef::Allocate(
            record.Data.Size() +
            sizeof (TMultiplexedRecordHeader));
        std::copy(
            reinterpret_cast<const char*>(&record.Header),
            reinterpret_cast<const char*>(&record.Header) + sizeof (TMultiplexedRecordHeader),
            multiplexedData.Begin());
        std::copy(
            record.Data.Begin(),
            record.Data.End(),
            multiplexedData.Begin() + sizeof (TMultiplexedRecordHeader));

        return MultiplexedChangelog_->Append(multiplexedData);
    }


    TErrorOr<IChangelogPtr> CreateNewMultiplexedChangelog(
        TAsyncError flushResult,
        int oldId,
        int newId)
    {
        auto flushError = WaitFor(flushResult);
        if (!flushError.IsOK()) {
            LOG_FATAL(flushError);
        }

        auto changelog = CreateMultiplexedChangelog(newId);

        LOG_INFO("Finished rotating multiplexed changelog %v",
            oldId);

        return changelog;
    }

    void WaitAndMarkMultplexedChangelogClean(
        const std::vector<TAsyncError>& results,
        int id)
    {
        for (const auto& result : results) {
            auto error = WaitFor(result);
            if (!error.IsOK()) {
                LOG_FATAL(error);
            }
        }

        MarkMultiplexedChangelogClean(id);
    }


    Stroka GetMultiplexedPath()
    {
        return Config_->MultiplexedChangelog->Path;
    }

    Stroka GetMultiplexedChangelogPath(int changelogId)
    {
        return NFS::CombinePaths(
            GetMultiplexedPath(),
            Format("%09d.%v", changelogId, ChangelogExtension));
    }


    void MarkMultiplexedChangelogClean(int changelogId)
    {
        auto dataFileName = GetMultiplexedChangelogPath(changelogId);
        auto cleanDataFileName = dataFileName + "." + CleanExtension;
        NFS::Rename(dataFileName, cleanDataFileName);
        NFS::Rename(dataFileName + "." + ChangelogIndexExtension, cleanDataFileName + "." + ChangelogIndexExtension);
        LOG_INFO("Multiplexed changelog %v is clean", changelogId);
    }

    IChangelogPtr CreateMultiplexedChangelog(int id)
    {
        LOG_INFO("Started creating new multiplexed changelog %v",
            id);

        auto changelog = ChangelogDispatcher_->CreateChangelog(
            GetMultiplexedChangelogPath(id),
            TChangelogMeta(),
            Config_->MultiplexedChangelog);

        LOG_INFO("Finished creating new multiplexed changelog %v",
            id);

        return changelog;
    }


    void OnCleanup()
    {
        try {
            auto fileNames = NFS::EnumerateFiles(GetMultiplexedPath());

            std::vector<int> ids;
            for (const auto& originalFileName : fileNames) {
                auto fileName = originalFileName;
                auto cleanExtension = ChopExtension(&fileName);
                if (cleanExtension != CleanExtension) 
                    continue;

                auto changelogExtension = ChopExtension(&fileName);
                if (changelogExtension != ChangelogExtension)
                    continue;

                int id = ParseChangelogId(fileName, originalFileName);
                ids.push_back(id);
            }

            if (ids.size() <= Config_->MultiplexedChangelog->MaxCleanChangelogsToKeep)
                return;

            std::sort(ids.begin(), ids.end());
            ids.erase(ids.end() - Config_->MultiplexedChangelog->MaxCleanChangelogsToKeep, ids.end());

            for (int id : ids) {
                LOG_INFO("Removing clean multiplexed changelog %v", id);

                auto fileName = GetMultiplexedChangelogPath(id) + "." + CleanExtension;
                RemoveChangelogFiles(fileName);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error cleaning up multiplexed changelogs");
        }
    }


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
        const TChunkId& chunkId,
        IChangelogPtr underlyingChangelog,
        bool enableMultiplexing)
        : TAsyncCacheValueBase<TChunkId, TCachedChangelog>(chunkId)
        , Owner_(owner)
        , EnableMultiplexing_(enableMultiplexing)
        , UnderlyingChangelog_(underlyingChangelog)
        , LastSplitFlushResult_(UnderlyingChangelog_->Flush())
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

    virtual TAsyncError Append(const TSharedRef& data) override
    {
        if (EnableMultiplexing_) {
            int recordId = UnderlyingChangelog_->GetRecordCount();
            LastSplitFlushResult_ = UnderlyingChangelog_->Append(data);

            // Construct the multiplexed data record.
            TMultiplexedRecord record;
            record.Header.Type = EMultiplexedRecordType::Append;
            record.Header.ChunkId = GetKey();
            record.Header.RecordId = recordId;
            record.Data = data;

            // Put the multiplexed record into the multiplexed changelog.
            return Owner_->AppendMultiplexedRecord(record, this);
        } else {
            return UnderlyingChangelog_->Append(data);
        }
    }

    virtual TAsyncError Flush() override
    {
        return UnderlyingChangelog_->Flush();
    }

    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        return UnderlyingChangelog_->Read(firstRecordId, maxRecords, maxBytes);
    }

    virtual TAsyncError Seal(int recordCount) override
    {
        return UnderlyingChangelog_->Seal(recordCount);
    }

    virtual TAsyncError Unseal() override
    {
        return UnderlyingChangelog_->Unseal();
    }

    virtual TAsyncError Close() override
    {
        Owner_->TryRemove(this);
        return UnderlyingChangelog_->Close();
    }

    TAsyncError GetLastSplitFlushResult() const
    {
        YASSERT(EnableMultiplexing_);
        return LastSplitFlushResult_;
    }

private:
    TImplPtr Owner_;
    bool EnableMultiplexing_;
    IChangelogPtr UnderlyingChangelog_;

    TAsyncError LastSplitFlushResult_;

};

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TMultiplexedReplay
{
public:
    explicit TMultiplexedReplay(TImplPtr owner)
        : Owner_(owner)
    { }

    int Run()
    {
        auto path = Owner_->GetMultiplexedPath();
        NFS::ForcePath(path);

        int minDirtyId = std::numeric_limits<int>::max();
        int maxDirtyId = std::numeric_limits<int>::min();
        int maxCleanId = std::numeric_limits<int>::min();

        auto fileNames = NFS::EnumerateFiles(path);
        for (const auto& originalFileName : fileNames) {
            auto fileName = originalFileName;
            auto extension = ChopExtension(&fileName);
            if (extension == CleanExtension) {
                extension = ChopExtension(&fileName);
                if (extension == ChangelogExtension) {
                    int id = ParseChangelogId(fileName, originalFileName);
                    LOG_INFO("Found clean multiplexed changelog %v", id);
                    maxCleanId = std::max(maxCleanId, id);
                }
            } else if (extension == ChangelogExtension) {
                int id = ParseChangelogId(fileName, originalFileName);
                LOG_INFO("Found dirty multiplexed changelog %v", id);
                minDirtyId = std::min(minDirtyId, id);
                maxDirtyId = std::max(maxDirtyId, id);
            }
        }

        for (int id = minDirtyId; id <= maxDirtyId; ++id) {
            ReplayChangelog(id);
        }

        for (const auto& pair : SplitMap_) {
            const auto& entry = pair.second;
            entry.Changelog->Close().Get();
            entry.Chunk->DetachChangelog();
        }

        if (maxDirtyId >= 0) {
            return maxDirtyId + 1;
        }

        if (maxCleanId >= 0) {
            return maxCleanId + 1;
        }

        return 0;
    }

private:
    TImplPtr Owner_;

    struct TSplitEntry
    {
        TSplitEntry(TJournalChunkPtr chunk, IChangelogPtr changelog)
            : Chunk(chunk)
            , Changelog(changelog)
        { }

        TJournalChunkPtr Chunk;
        IChangelogPtr Changelog;
        int RecordsAdded = 0;
    };

    yhash_map<TChunkId, TSplitEntry> SplitMap_;


    void ReplayChangelog(int changelogId)
    {
        LOG_INFO("Replaying dirty multiplexed changelog %v", changelogId);

        auto multiplexedChangelogPath = Owner_->GetMultiplexedChangelogPath(changelogId);
        auto multiplexedChangelog = Owner_->ChangelogDispatcher_->OpenChangelog(
            multiplexedChangelogPath,
            Owner_->Config_->MultiplexedChangelog);

        int startRecordId = 0;
        int recordCount = multiplexedChangelog->GetRecordCount();
        while (startRecordId < recordCount) {
            auto records = multiplexedChangelog->Read(
                startRecordId,
                recordCount,
                Owner_->Config_->MultiplexedChangelog->ReplayBufferSize);

            for (const auto& record : records) {
                YCHECK(record.Size() >= sizeof (TMultiplexedRecordHeader));
                auto* header = reinterpret_cast<const TMultiplexedRecordHeader*>(record.Begin());
                ReplayRecord(record, *header);
            }

            startRecordId += records.size();
        }

        for (auto& pair : SplitMap_) {
            auto& entry = pair.second;
                
            LOG_INFO("Started flushing journal chunk (ChunkId: %v)",
                pair.first);

            entry.Changelog->Flush().Get();

            LOG_INFO("Finished flushing journal chunk (ChunkId: %v, RecordAdded: %v)",
                pair.first,
                entry.RecordsAdded);

            entry.RecordsAdded = 0;
        }

        Owner_->MarkMultiplexedChangelogClean(changelogId);
    }

    void ReplayRecord(const TSharedRef& record, const TMultiplexedRecordHeader& header)
    {
        switch (header.Type) {
            case EMultiplexedRecordType::Append:
                ReplayAppendRecord(record, header);
                break;
            case EMultiplexedRecordType::Create:
                ReplayCreateRecord(header);
                break;
            case EMultiplexedRecordType::Remove:
                ReplayRemoveRecord(header);
                break;
            default:
                YUNREACHABLE();
        }
    }

    void ReplayAppendRecord(const TSharedRef& record, const TMultiplexedRecordHeader& header)
    {
        auto* splitEntry = FindSplitEntry(header.ChunkId);
        if (!splitEntry)
            return;

        if (splitEntry->Changelog->IsSealed())
            return;

        int recordCount = splitEntry->Changelog->GetRecordCount();
        if (recordCount > header.RecordId)
            return;

        YCHECK(recordCount == header.RecordId);
        auto splitRecord = record.Slice(TRef(
            const_cast<char*>(record.Begin() + sizeof (TMultiplexedRecordHeader)),
            const_cast<char*>(record.End())));
        splitEntry->Changelog->Append(splitRecord);
        ++splitEntry->RecordsAdded;
    }

    void ReplayCreateRecord(const TMultiplexedRecordHeader& header)
    {
        const auto& chunkId = header.ChunkId;
        auto chunkStore = Owner_->Bootstrap_->GetChunkStore();
        if (chunkStore->FindChunk(chunkId))
            return;

        auto location = chunkStore->GetNewChunkLocation();

        auto chunk = New<TJournalChunk>(
            Owner_->Bootstrap_,
            location,
            TChunkDescriptor(chunkId));
        chunkStore->RegisterNewChunk(chunk);

        Owner_->DoCreateChangelog(chunk);
    }

    void ReplayRemoveRecord(const TMultiplexedRecordHeader& header)
    {
        const auto& chunkId = header.ChunkId;
        auto chunkStore = Owner_->Bootstrap_->GetChunkStore();
        auto chunk = chunkStore->FindChunk(chunkId);
        if (!chunk)
            return;

        auto journalChunk = chunk->AsJournalChunk();
        Owner_->DoRemoveChangelog(journalChunk);
        chunkStore->UnregisterChunk(chunk);
    }


    TSplitEntry* FindSplitEntry(const TChunkId& chunkId)
    {
        auto it = SplitMap_.find(chunkId);
        if (it == SplitMap_.end()) {
            auto chunkStore = Owner_->Bootstrap_->GetChunkStore();
            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                return nullptr;
            }

            auto journalChunk = chunk->AsJournalChunk();
            auto location = journalChunk->GetLocation();
            auto asyncChangelog = Owner_->OpenChangelog(
                location,
                chunkId,
                false);

            auto changelogOrError = asyncChangelog.Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
            auto changelog = changelogOrError.Value();

            journalChunk->AttachChangelog(changelog);
            it = SplitMap_.insert(std::make_pair(
                chunkId,
                TSplitEntry(journalChunk, changelog))).first;
        }
        return &it->second;
    }

};

////////////////////////////////////////////////////////////////////////////////

void TJournalDispatcher::TImpl::Initialize()
{
    LOG_INFO("Starting journal dispatcher");

    try {
        if (Config_->MultiplexedChangelog) {
            // Initialize and replay multiplexed changelogs.
            TMultiplexedReplay replay(this);
            int newId = replay.Run();

            // Create new multiplexed changelog.
            MultiplexedChangelog_ = CreateMultiplexedChangelog(newId);
            MultiplexedChangelogId_ = newId;
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error starting journal dispatcher")
            << ex;
    }

    CleanupExecutor_ = New<TPeriodicExecutor>(
        GetHydraIOInvoker(),
        BIND(&TImpl::OnCleanup, MakeWeak(this)),
        CleanupPeriod);
    CleanupExecutor_->Start();

    LOG_INFO("Journal dispatcher started");
}

TFuture<TErrorOr<IChangelogPtr>> TJournalDispatcher::TImpl::OpenChangelog(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    TInsertCookie cookie(chunkId);
    bool inserted = BeginInsert(&cookie);
    auto result = cookie.GetValue();

    if (inserted) {
        // NB: Changelogs are opened, created, closed and removed within changelog's dispatcher
        // invoker to ensure proper ordering of these events.
        ChangelogDispatcher_->GetInvoker()->Invoke(BIND(
            &TImpl::DoOpenChangelog,
            MakeStrong(this),
            location,
            chunkId,
            enableMultiplexing,
            Passed(std::move(cookie))));
    }

    return result.Apply(BIND([] (const TErrorOr<TCachedChangelogPtr>& result) {
        return result.As<IChangelogPtr>();
    }));
}

TFuture<TErrorOr<IChangelogPtr>> TJournalDispatcher::TImpl::CreateChangelog(
    TJournalChunkPtr chunk,
    bool enableMultiplexing)
{
    try {
        if (!AcceptsChunks()) {
            THROW_ERROR_EXCEPTION("No new journal chunks are accepted");
        }

        const auto& chunkId = chunk->GetId();

        TInsertCookie cookie(chunkId);
        if (!BeginInsert(&cookie)) {
            THROW_ERROR_EXCEPTION("Journal chunk %v is still busy",
                chunkId);
        }

        // See remark on invoker choice above.
        auto futureChangelogOrError = BIND(&TImpl::DoCreateChangelog, MakeStrong(this), chunk)
            .Guarded()
            .AsyncVia(ChangelogDispatcher_->GetInvoker())
            .Run();

        auto lazyChangelog = CreateLazyChangelog(futureChangelogOrError);

        auto cachedChangelog = New<TCachedChangelog>(
            this,
            chunkId,
            lazyChangelog,
            enableMultiplexing);
        cookie.EndInsert(cachedChangelog);

        if (enableMultiplexing) {
            TMultiplexedRecord record;
            record.Header.Type = EMultiplexedRecordType::Create;
            record.Header.ChunkId = chunkId;
            record.Header.RecordId = -1;
            auto multiplexedResult = AppendMultiplexedRecord(record, cachedChangelog);

            return multiplexedResult.Apply(BIND([=] (const TError& error) -> TErrorOr<IChangelogPtr> {
                return error.IsOK() ? TErrorOr<IChangelogPtr>(cachedChangelog) : TErrorOr<IChangelogPtr>(error);
            }));
        } else {
            return futureChangelogOrError.Apply(BIND([=] (const TErrorOr<IChangelogPtr>& result) {
                return result.IsOK() ? TErrorOr<IChangelogPtr>(cachedChangelog) : TErrorOr<IChangelogPtr>(result);
            }));
        }
    } catch (const std::exception& ex) {
        return MakeFuture<TErrorOr<IChangelogPtr>>(ex);
    }
}

TAsyncError TJournalDispatcher::TImpl::RemoveChangelog(TJournalChunkPtr chunk)
{
    TMultiplexedRecord record;
    record.Header.Type = EMultiplexedRecordType::Remove;
    record.Header.ChunkId = chunk->GetId();
    record.Header.RecordId = -1;
    AppendMultiplexedRecord(record, nullptr);

    {
        TGuard<TSpinLock> guard(SpinLock_);
        RemoveChangelogFromActive(chunk->GetId());
    }

    // See remark on invoker choice above.
    return BIND(&TImpl::DoRemoveChangelog, MakeStrong(this), chunk)
        .Guarded()
        .AsyncVia(ChangelogDispatcher_->GetInvoker())
        .Run();
}

TAsyncError TJournalDispatcher::TImpl::AppendMultiplexedRecord(
    const TMultiplexedRecord& record,
    TCachedChangelogPtr changelog)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (changelog) {
        AddChangelogToActive(changelog->GetKey(), changelog);
    }

    // Construct the multiplexed data record and append it.
    auto appendResult = DoAppendMultiplexedRecord(record);

    // Check if it is time to rotate.
    const auto& config = Config_->MultiplexedChangelog;
    if (MultiplexedChangelog_->GetRecordCount() >= config->MaxRecordCount ||
        MultiplexedChangelog_->GetDataSize() >= config->MaxDataSize)
    {
        LOG_INFO("Started rotating multiplexed changelog %v",
            MultiplexedChangelogId_);

        auto multiplexedFlushResult = MultiplexedChangelog_->Flush();

        // To mark a multiplexed changelog as clean we wait for
        // * the multiplexed changelog to get flushed
        // * last appended records in all active changelogs to get flushed
        std::vector<TAsyncError> cleanResults;
        cleanResults.push_back(multiplexedFlushResult);
        for (const auto& pair : ActiveChangelogs_) {
            const auto& changelog = pair.second;
            cleanResults.push_back(changelog->GetLastSplitFlushResult());
        }
        ClearActiveChangelogs();

        guard.Release();

        int oldId = MultiplexedChangelogId_;
        int newId = MultiplexedChangelogId_ + 1;

        auto futureMultiplexedChangelogOrError =
            BIND(
                &TImpl::CreateNewMultiplexedChangelog,
                MakeStrong(this),
                multiplexedFlushResult,
                oldId,
                newId)
            .AsyncVia(ChangelogDispatcher_->GetInvoker())
            .Run();

        BIND(
            &TImpl::WaitAndMarkMultplexedChangelogClean,
            MakeStrong(this),
            cleanResults,
            oldId)
        .AsyncVia(ChangelogDispatcher_->GetInvoker())
        .Run();
        
        MultiplexedChangelog_ = CreateLazyChangelog(futureMultiplexedChangelogOrError);
        MultiplexedChangelogId_ = newId;
    }

    return appendResult;
}

void TJournalDispatcher::TImpl::OnAdded(TCachedChangelog* changelog)
{
    LOG_DEBUG("Journal chunk added to cache (ChunkId: %v)",
        changelog->GetKey());
}

void TJournalDispatcher::TImpl::OnRemoved(TCachedChangelog* changelog)
{
    LOG_DEBUG("Journal chunk evicted from cache (ChunkId: %v)",
        changelog->GetKey());
}

////////////////////////////////////////////////////////////////////////////////

TJournalDispatcher::TJournalDispatcher(
    NCellNode::TBootstrap* bootstrap,
    TDataNodeConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, config))
{ }

TJournalDispatcher::~TJournalDispatcher()
{ }

void TJournalDispatcher::Initialize()
{
    Impl_->Initialize();
}

bool TJournalDispatcher::AcceptsChunks() const
{
    return Impl_->AcceptsChunks();
}

TFuture<TErrorOr<IChangelogPtr>> TJournalDispatcher::OpenChangelog(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    return Impl_->OpenChangelog(location, chunkId, enableMultiplexing);
}

TFuture<TErrorOr<IChangelogPtr>> TJournalDispatcher::CreateChangelog(
    TJournalChunkPtr chunk,
    bool enableMultiplexing)
{
    return Impl_->CreateChangelog(chunk, enableMultiplexing);
}

TAsyncError TJournalDispatcher::RemoveChangelog(TJournalChunkPtr chunk)
{
    return Impl_->RemoveChangelog(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
