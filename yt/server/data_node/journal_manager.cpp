#include "stdafx.h"
#include "journal_manager.h"
#include "journal_dispatcher.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "chunk_store.h"
#include "journal_chunk.h"

#include <core/misc/enum.h>
#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/periodic_executor.h>

#include <core/profiling/timing.h>

#include <ytlib/hydra/hydra_manager.pb.h>

#include <server/hydra/changelog.h>
#include <server/hydra/file_changelog_dispatcher.h>
#include <server/hydra/lazy_changelog.h>
#include <server/hydra/file_helpers.h>
#include <server/hydra/private.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NHydra;
using namespace NHydra::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto CleanupPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMultiplexedRecordType,
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

struct TMultiplexedChangelogDescriptor
{
    int Id;
    bool Clean;
};

struct IMultiplexedReplayerCallbacks
{
    virtual ~IMultiplexedReplayerCallbacks()
    { }

    virtual std::vector<TMultiplexedChangelogDescriptor> ListMultiplexedChangelogs() = 0;
    virtual IChangelogPtr OpenMultiplexedChangelog(int id) = 0;
    virtual void MarkMultiplexedChangelogClean(int id) = 0;

    virtual IChangelogPtr CreateSplitChangelog(const TChunkId& chunkId) = 0;
    virtual IChangelogPtr OpenSplitChangelog(const TChunkId& chunkId) = 0;
    virtual void FlushSplitChangelog(const TChunkId& chunkId) = 0;
    virtual bool RemoveSplitChangelog(const TChunkId& chunkId) = 0;
};

class TMultiplexedReplayer
    : public TRefCounted
{
public:
    TMultiplexedReplayer(
        TMultiplexedChangelogConfigPtr config,
        IMultiplexedReplayerCallbacks* callbacks,
        const NLogging::TLogger& logger)
        : Config_(config)
        , Callbacks_(callbacks)
        , Logger(logger)
    { }

    int ReplayChangelogs()
    {
        int minDirtyId = std::numeric_limits<int>::max();
        int maxDirtyId = std::numeric_limits<int>::min();
        int maxCleanId = std::numeric_limits<int>::min();

        auto descriptors = Callbacks_->ListMultiplexedChangelogs();
        for (const auto& descriptor : descriptors) {
            int id = descriptor.Id;
            if (descriptor.Clean) {
                LOG_INFO("Found clean multiplexed changelog %v", id);
                maxCleanId = std::max(maxCleanId, id);
            } else {
                LOG_INFO("Found dirty multiplexed changelog %v", id);
                minDirtyId = std::min(minDirtyId, id);
                maxDirtyId = std::max(maxDirtyId, id);
            }
        }

        for (int id = minDirtyId; id <= maxDirtyId; ++id) {
            AnalyzeChangelog(id);
            ReplayChangelog(id);
        }

        for (const auto& pair : SplitMap_) {
            const auto& entry = pair.second;
            Callbacks_->FlushSplitChangelog(entry.ChunkId);
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
    const TMultiplexedChangelogConfigPtr Config_;
    IMultiplexedReplayerCallbacks* const Callbacks_;
    const NLogging::TLogger Logger;

    yhash_set<TChunkId> CreateChunkIds_;
    yhash_set<TChunkId> RemoveChunkIds_;
    yhash_set<TChunkId> AppendChunkIds_;
    yhash_map<TChunkId, int> ChunkIdToFirstRelevantRecordId_;

    struct TSplitEntry
    {
        TSplitEntry(const TChunkId& chunkId, IChangelogPtr changelog)
            : ChunkId(chunkId)
            , Changelog(changelog)
        { }

        TChunkId ChunkId;
        IChangelogPtr Changelog;
        int RecordsAdded = 0;
    };

    yhash_map<TChunkId, TSplitEntry> SplitMap_;


    int GetFirstRelevantRecordId(const TChunkId& chunkId)
    {
        auto it = ChunkIdToFirstRelevantRecordId_.find(chunkId);
        YCHECK(it != ChunkIdToFirstRelevantRecordId_.end());
        return it->second;
    }

    void ScanChangelog(
        int changelogId,
        const std::function<void(int recordId, const TMultiplexedRecord& record)>& handler)
    {
        int startRecordId = 0;
        auto multiplexedChangelog = Callbacks_->OpenMultiplexedChangelog(changelogId);
        int recordCount = multiplexedChangelog->GetRecordCount();
        while (startRecordId < recordCount) {
            auto asyncRecordsData = multiplexedChangelog->Read(
                startRecordId,
                recordCount,
                Config_->ReplayBufferSize);
            auto recordsData = WaitFor(asyncRecordsData)
                .ValueOrThrow();

            int currentRecordId = startRecordId;
            for (const auto& recordData : recordsData) {
                YCHECK(recordData.Size() >= sizeof (TMultiplexedRecordHeader));
                TMultiplexedRecord record;
                record.Header = *reinterpret_cast<const TMultiplexedRecordHeader*>(recordData.Begin());
                record.Data = recordData.Slice(sizeof (TMultiplexedRecordHeader), recordData.Size());
                handler(currentRecordId, record);
                ++currentRecordId;
            }

            startRecordId += recordsData.size();
        }
        multiplexedChangelog->Close()
            .Get()
            .ThrowOnError();
    }

    void AnalyzeChangelog(int changelogId)
    {
        LOG_INFO("Analyzing dirty multiplexed changelog %v", changelogId);

        CreateChunkIds_.clear();
        RemoveChunkIds_.clear();
        AppendChunkIds_.clear();
        ChunkIdToFirstRelevantRecordId_.clear();

        ScanChangelog(changelogId, [&] (int recordId, const TMultiplexedRecord& record) {
            const auto& chunkId = record.Header.ChunkId;
            switch (record.Header.Type) {
                case EMultiplexedRecordType::Append: {
                    YCHECK(RemoveChunkIds_.find(chunkId) == RemoveChunkIds_.end());
                    auto it = ChunkIdToFirstRelevantRecordId_.find(chunkId);
                    if (it == ChunkIdToFirstRelevantRecordId_.end()) {
                        YCHECK(ChunkIdToFirstRelevantRecordId_.insert(std::make_pair(chunkId, recordId)).second);
                    }
                    AppendChunkIds_.insert(chunkId);
                    break;
                }

                case EMultiplexedRecordType::Create:
                    YCHECK(AppendChunkIds_.find(chunkId) == AppendChunkIds_.end());
                    YCHECK(CreateChunkIds_.find(chunkId) == CreateChunkIds_.end());
                    CreateChunkIds_.insert(chunkId);
                    RemoveChunkIds_.erase(chunkId);
                    ChunkIdToFirstRelevantRecordId_[chunkId] = recordId;
                    break;

                case EMultiplexedRecordType::Remove:
                    YCHECK(RemoveChunkIds_.find(chunkId) == RemoveChunkIds_.end());
                    RemoveChunkIds_.insert(chunkId);
                    CreateChunkIds_.erase(chunkId);
                    AppendChunkIds_.erase(chunkId);
                    ChunkIdToFirstRelevantRecordId_[chunkId] = recordId;
                    break;
                default:
                    YUNREACHABLE();
            }
        });

        auto dumpChunkIds = [&] (const yhash_set<TChunkId>& chunkIds, const Stroka& action) {
            for (const auto& chunkId : chunkIds) {
                LOG_INFO("Replay may %v journal chunk (ChunkId: %v, FirstRelevantRecordId: %v)",
                    action,
                    chunkId,
                    GetFirstRelevantRecordId(chunkId));
            }
        };

        dumpChunkIds(CreateChunkIds_, "create");
        dumpChunkIds(AppendChunkIds_, "append to");
        dumpChunkIds(RemoveChunkIds_, "remove");
    }

    void ReplayChangelog(int changelogId)
    {
        LOG_INFO("Replaying dirty multiplexed changelog %v", changelogId);

        ScanChangelog(changelogId, [&] (int recordId, const TMultiplexedRecord& record) {
            const auto& chunkId = record.Header.ChunkId;
            if (recordId < GetFirstRelevantRecordId(chunkId))
                return;

            switch (record.Header.Type) {
                case EMultiplexedRecordType::Append:
                    ReplayAppendRecord(record);
                    break;

                case EMultiplexedRecordType::Create:
                    ReplayCreateRecord(record);
                    break;

                case EMultiplexedRecordType::Remove:
                    ReplayRemoveRecord(record);
                    break;

                default:
                    YUNREACHABLE();
            }
        });

        for (auto& pair : SplitMap_) {
            auto& entry = pair.second;
            if (entry.RecordsAdded == 0)
                continue;

            entry.Changelog->Flush()
                .Get()
                .ThrowOnError();

            LOG_INFO("Replay appended to journal chunk (ChunkId: %v, RecordsAdded: %v)",
                pair.first,
                entry.RecordsAdded);

            entry.RecordsAdded = 0;
        }

        Callbacks_->MarkMultiplexedChangelogClean(changelogId);
    }

    void ReplayAppendRecord(const TMultiplexedRecord& record)
    {
        const auto& chunkId = record.Header.ChunkId;

        auto it = SplitMap_.find(chunkId);
        if (it == SplitMap_.end()) {
            auto changelog = Callbacks_->OpenSplitChangelog(chunkId);
            if (!changelog) {
                LOG_FATAL("Journal chunk %v is missing but has relevant records in the multiplexed changelog",
                    chunkId);
            }
            it = SplitMap_.insert(std::make_pair(
                chunkId,
                TSplitEntry(chunkId, changelog))).first;
        }

        auto& splitEntry = it->second;

        if (splitEntry.Changelog->IsSealed())
            return;

        int recordCount = splitEntry.Changelog->GetRecordCount();
        if (recordCount > record.Header.RecordId)
            return;

        if (recordCount != record.Header.RecordId) {
            LOG_FATAL("Journal chunk %v has %v records while multiplexed changelog has relevant records starting from %v",
                record.Header.ChunkId,
                recordCount,
                record.Header.RecordId);
        }

        splitEntry.Changelog->Append(record.Data);
        ++splitEntry.RecordsAdded;
    }

    void ReplayCreateRecord(const TMultiplexedRecord& record)
    {
        const auto& chunkId = record.Header.ChunkId;

        auto changelog = Callbacks_->CreateSplitChangelog(chunkId);
        if (!changelog) {
            LOG_INFO("Journal chunk creation skipped since the chunk already exists (ChunkId: %v)",
                chunkId);
            return;
        }

        YCHECK(SplitMap_.insert(std::make_pair(
            chunkId,
            TSplitEntry(chunkId, changelog))).second);

        LOG_INFO("Replay created journal chunk (ChunkId: %v)",
            chunkId);
    }

    void ReplayRemoveRecord(const TMultiplexedRecord& record)
    {
        const auto& chunkId = record.Header.ChunkId;

        YCHECK(SplitMap_.find(chunkId) == SplitMap_.end());

        if (!Callbacks_->RemoveSplitChangelog(chunkId))
            return;

        LOG_INFO("Replay removed journal chunk (ChunkId: %v)",
            chunkId);
    }

};

class TMultiplexedWriter
    : public TRefCounted
{
public:
    TMultiplexedWriter(
        TMultiplexedChangelogConfigPtr config,
        TFileChangelogDispatcherPtr changelogDispatcher,
        const Stroka& path,
        const NLogging::TLogger& logger)
        : Config_(config)
        , ChangelogDispatcher_(changelogDispatcher)
        , Path_(path)
        , Logger(logger)
    { }

    void Initialize(int changelogId)
    {
        auto changelog = CreateMultiplexedChangelog(changelogId);
        SetMultiplexedChangelog(changelog, changelogId);

        CleanupExecutor_ = New<TPeriodicExecutor>(
            GetHydraIOInvoker(),
            BIND(&TMultiplexedWriter::OnCleanup, MakeWeak(this)),
            CleanupPeriod);
        CleanupExecutor_->Start();
    }

    TFuture<void> WriteCreateRecord(const TChunkId& chunkId)
    {
        TMultiplexedRecord record;
        record.Header.Type = EMultiplexedRecordType::Create;
        record.Header.ChunkId = chunkId;
        record.Header.RecordId = -1;
        return DoAppendMultiplexedRecord(record, Null);
    }

    TFuture<void> WriteRemoveRecord(const TChunkId& chunkId)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            RemoveChangelogFromActive(chunkId);
        }

        TMultiplexedRecord record;
        record.Header.Type = EMultiplexedRecordType::Remove;
        record.Header.ChunkId = chunkId;
        record.Header.RecordId = -1;
        return DoAppendMultiplexedRecord(record, Null);
    }

    TFuture<void> WriteAppendRecord(
        const TChunkId& chunkId,
        int recordId,
        const TSharedRef& recordData,
        TFuture<void> flushResult)
    {
        TMultiplexedRecord record;
        record.Header.Type = EMultiplexedRecordType::Append;
        record.Header.RecordId = recordId;
        record.Header.ChunkId = chunkId;
        record.Data = recordData;
        return DoAppendMultiplexedRecord(record, std::move(flushResult));
    }

    std::vector<TMultiplexedChangelogDescriptor> ListMultiplexedChangelogs()
    {
        NFS::ForcePath(Path_);
        auto fileNames = NFS::EnumerateFiles(Path_);
        std::vector<TMultiplexedChangelogDescriptor> result;
        for (const auto& originalFileName : fileNames) {
            auto fileName = originalFileName;
            auto extension = ChopExtension(&fileName);
            if (extension == CleanExtension) {
                extension = ChopExtension(&fileName);
                if (extension == ChangelogExtension) {
                    int id = ParseChangelogId(fileName, originalFileName);
                    result.push_back(TMultiplexedChangelogDescriptor{id, true});
                }
            } else if (extension == ChangelogExtension) {
                int id = ParseChangelogId(fileName, originalFileName);
                result.push_back(TMultiplexedChangelogDescriptor{id, false});
            }
        }
        return result;
    }

    IChangelogPtr OpenMultiplexedChangelog(int changelogId)
    {
        auto path = GetMultiplexedChangelogPath(changelogId);
        return ChangelogDispatcher_->OpenChangelog(path, Config_);
    }

    void MarkMultiplexedChangelogClean(int changelogId)
    {
        auto dataFileName = GetMultiplexedChangelogPath(changelogId);
        auto cleanDataFileName = dataFileName + "." + CleanExtension;
        NFS::Rename(dataFileName, cleanDataFileName);
        NFS::Rename(dataFileName + ChangelogIndexSuffix, cleanDataFileName + ChangelogIndexSuffix);
        LOG_INFO("Multiplexed changelog %v is clean", changelogId);
    }

private:
    const TMultiplexedChangelogConfigPtr Config_;
    const TFileChangelogDispatcherPtr ChangelogDispatcher_;
    const Stroka Path_;
    const NLogging::TLogger Logger;

    //! Protects a section of members.
    TSpinLock SpinLock_;

    //! The current multiplexed changelog.
    IChangelogPtr MultiplexedChangelog_;

    //! The moment when the multiplexed changelog was last rotated.
    NProfiling::TCpuInstant MultiplexedChangelogRotationDeadline_;

    //! The id of #MultiplexedChangelog.
    int MultiplexedChangelogId_;

    //! Maps journal chunk id to the latest flush results for changelogs whose records
    //! were added into the current multiplexed changelog.
    //! Safeguards marking multiplexed changelogs as clean.
    yhash_map<TChunkId, TFuture<void>> ChunkIdToFlushResult_;

    TPeriodicExecutorPtr CleanupExecutor_;


    void AddChangelogToActive(const TChunkId& chunkId, TFuture<void> flushResult)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (ChunkIdToFlushResult_.insert(std::make_pair(chunkId, flushResult)).second) {
            LOG_DEBUG("Changelog is added to active set (ChunkId: %v)",
                chunkId);
        }
    }

    void RemoveChangelogFromActive(const TChunkId& chunkId)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (ChunkIdToFlushResult_.erase(chunkId) == 1) {
            LOG_DEBUG("Changelog is removed from active set (ChunkId: %v)",
                chunkId);
        }
    }

    void ClearActiveChangelogs()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        for (const auto& pair : ChunkIdToFlushResult_) {
            LOG_DEBUG("Changelog is removed from active set (ChunkId: %v)",
                pair.first);
        }

        ChunkIdToFlushResult_.clear();
    }

    TFuture<void> DoAppendMultiplexedRecord(
        const TMultiplexedRecord& record,
        TFuture<void> flushResult)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);

        if (flushResult) {
            AddChangelogToActive(record.Header.ChunkId, flushResult);
        }

        // Construct the multiplexed data record and append it.
        auto multiplexedData = TSharedMutableRef::Allocate(
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
        auto appendResult = MultiplexedChangelog_->Append(multiplexedData);

        // Check if it is time to rotate.
        if (MultiplexedChangelog_->GetRecordCount() >= Config_->MaxRecordCount ||
            MultiplexedChangelog_->GetDataSize() >= Config_->MaxDataSize ||
            NProfiling::GetCpuInstant() > MultiplexedChangelogRotationDeadline_)
        {
            LOG_INFO("Started rotating multiplexed changelog %v",
                MultiplexedChangelogId_);

            auto multiplexedFlushResult = MultiplexedChangelog_->Flush();

            // To mark a multiplexed changelog as clean we wait for
            // * the multiplexed changelog to get flushed
            // * last appended records in all active changelogs to get flushed
            std::vector<TFuture<void>> cleanResults;
            cleanResults.push_back(multiplexedFlushResult);
            for (const auto& pair : ChunkIdToFlushResult_) {
                cleanResults.push_back(pair.second);
            }
            ClearActiveChangelogs();

            guard.Release();

            int oldId = MultiplexedChangelogId_;
            int newId = MultiplexedChangelogId_ + 1;

            auto futureMultiplexedChangelog =
                BIND(
                    &TMultiplexedWriter::CreateNewMultiplexedChangelog,
                    MakeStrong(this),
                    multiplexedFlushResult,
                    oldId,
                    newId)
                .AsyncVia(ChangelogDispatcher_->GetInvoker())
                .Run();

            BIND(
                &TMultiplexedWriter::WaitAndMarkMultplexedChangelogClean,
                MakeStrong(this),
                cleanResults,
                oldId)
            .AsyncVia(ChangelogDispatcher_->GetInvoker())
            .Run();

            SetMultiplexedChangelog(CreateLazyChangelog(futureMultiplexedChangelog), newId);
        }

        return appendResult;
    }

    void SetMultiplexedChangelog(IChangelogPtr changelog, int id)
    {
        MultiplexedChangelog_ = changelog;
        MultiplexedChangelogId_ = id;
        MultiplexedChangelogRotationDeadline_ =
            NProfiling::GetCpuInstant() +
            NProfiling::DurationToCpuDuration(Config_->AutoRotationPeriod);
    }

    IChangelogPtr CreateMultiplexedChangelog(int id)
    {
        LOG_INFO("Started creating new multiplexed changelog %v",
            id);

        auto changelog = ChangelogDispatcher_->CreateChangelog(
            GetMultiplexedChangelogPath(id),
            TChangelogMeta(),
            Config_);

        LOG_INFO("Finished creating new multiplexed changelog %v",
            id);

        return changelog;
    }

    IChangelogPtr CreateNewMultiplexedChangelog(
        TFuture<void> flushResult,
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
        const std::vector<TFuture<void>>& results,
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

    void OnCleanup()
    {
        try {
            auto fileNames = NFS::EnumerateFiles(Path_);

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

            if (ids.size() <= Config_->MaxCleanChangelogsToKeep)
                return;

            std::sort(ids.begin(), ids.end());
            ids.erase(ids.end() - Config_->MaxCleanChangelogsToKeep, ids.end());

            for (int id : ids) {
                LOG_INFO("Removing clean multiplexed changelog %v", id);

                auto fileName = GetMultiplexedChangelogPath(id) + "." + CleanExtension;
                RemoveChangelogFiles(fileName);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error cleaning up multiplexed changelogs");
        }
    }

    Stroka GetMultiplexedChangelogPath(int changelogId)
    {
        return NFS::CombinePaths(
            Path_,
            Format("%09d.%v", changelogId, ChangelogExtension));
    }


};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJournalManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TDataNodeConfigPtr config,
        TLocation* location,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Location_(location)
        , Bootstrap_(bootstrap)
    {
        Logger = DataNodeLogger;
        Logger.AddTag("LocationId: %v", Location_->GetId());

        ChangelogDispatcher_ = New<TFileChangelogDispatcher>(
            Config_->MultiplexedChangelog,
            "Flush:" + Location_->GetId());

        MultiplexedWriter_ = New<TMultiplexedWriter>(
            Config_->MultiplexedChangelog,
            ChangelogDispatcher_,
            NFS::CombinePaths(Location_->GetPath(), MultiplexedDirectory),
            Logger);
    }

    void Initialize()
    {
        LOG_INFO("Initializing journals");

        // Initialize and replay multiplexed changelogs.
        TMultiplexedReplayCallbacks replayCallbacks(this);
        auto replayer = New<TMultiplexedReplayer>(
            Config_->MultiplexedChangelog,
            &replayCallbacks,
            Logger);
        int newId = replayer->ReplayChangelogs();

        // Create new multiplexed changelog.
        MultiplexedWriter_->Initialize(newId);

        LOG_INFO("Journals initialized");
    }

    TFuture<IChangelogPtr> OpenChangelog(const TChunkId& chunkId)
    {
        return
            BIND(&TImpl::DoOpenChangelog, MakeStrong(this), chunkId)
            .AsyncVia(ChangelogDispatcher_->GetInvoker())
            .Run();
    }

    TFuture<IChangelogPtr> CreateChangelog(
        const TChunkId& chunkId,
        bool enableMultiplexing)
    {
        auto futureChangelog = BIND(&TImpl::DoCreateChangelog, MakeStrong(this), chunkId)
            .AsyncVia(ChangelogDispatcher_->GetInvoker())
            .Run();

        if (!enableMultiplexing) {
            return futureChangelog;
        }

        return MultiplexedWriter_->WriteCreateRecord(chunkId)
            .Apply(BIND(&CreateLazyChangelog, std::move(futureChangelog)));
    }

    TFuture<void> RemoveChangelog(
        TJournalChunkPtr chunk,
        bool enableMultiplexing)
    {
        if (enableMultiplexing) {
            MultiplexedWriter_->WriteRemoveRecord(chunk->GetId());
        }

        return BIND(&TImpl::DoRemoveChangelog, MakeStrong(this), chunk)
            .AsyncVia(ChangelogDispatcher_->GetInvoker())
            .Run();
    }

    TFuture<void> AppendMultiplexedRecord(
        const TChunkId& chunkId,
        int recordId,
        const TSharedRef& recordData,
        TFuture<void> flushResult)
    {
        return MultiplexedWriter_->WriteAppendRecord(
            chunkId,
            recordId,
            recordData,
            std::move(flushResult));
    }

private:
    const TDataNodeConfigPtr Config_;
    TLocation* const Location_;
    NCellNode::TBootstrap* const Bootstrap_;

    TFileChangelogDispatcherPtr ChangelogDispatcher_;
    TIntrusivePtr<TMultiplexedWriter> MultiplexedWriter_;

    NLogging::TLogger Logger;


    IChangelogPtr DoCreateChangelog(const TChunkId& chunkId)
    {
        IChangelogPtr changelog;

        LOG_DEBUG("Started creating journal chunk (ChunkId: %v)",
            chunkId);

        const auto& Profiler = Location_->GetProfiler();
        PROFILE_TIMING("/journal_chunk_create_time") {
            try {
                auto fileName = Location_->GetChunkPath(chunkId);
                changelog = ChangelogDispatcher_->CreateChangelog(
                    fileName,
                    TChangelogMeta(),
                    Config_->SplitChangelog);
            } catch (const std::exception& ex) {
                auto error = TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error creating journal chunk %v",
                    chunkId) << ex;
                Location_->Disable(error);
                THROW_ERROR error;
            }
        }

        LOG_DEBUG("Finished creating journal chunk (ChunkId: %v)",
            chunkId);

        return changelog;
    }

    IChangelogPtr DoOpenChangelog(const TChunkId& chunkId)
    {
        IChangelogPtr changelog;

        LOG_TRACE("Started opening journal chunk (ChunkId: %v)",
            chunkId);

        const auto& Profiler = Location_->GetProfiler();
        PROFILE_TIMING("/journal_chunk_open_time") {
            try {
                auto fileName = Location_->GetChunkPath(chunkId);
                changelog = ChangelogDispatcher_->OpenChangelog(
                    fileName,
                    Config_->SplitChangelog);
            } catch (const std::exception& ex) {
                auto error = TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error opening journal chunk %v",
                    chunkId)
                    << ex;
                Location_->Disable(error);
                THROW_ERROR error;
            }
        }

        LOG_TRACE("Finished opening journal chunk (ChunkId: %v)",
            chunkId);

        return changelog;
    }

    void DoRemoveChangelog(TJournalChunkPtr chunk)
    {
        const auto& Profiler = Location_->GetProfiler();
        PROFILE_TIMING("/journal_chunk_remove_time") {
            chunk->SyncRemove(false);
        }
    }


    class TMultiplexedReplayCallbacks
        : public IMultiplexedReplayerCallbacks
    {
    public:
        explicit TMultiplexedReplayCallbacks(TImpl* impl)
            : Impl_(impl)
        { }

        // IMultiplexedReplayerCallbacks
        virtual std::vector<TMultiplexedChangelogDescriptor> ListMultiplexedChangelogs() override
        {
            return Impl_->MultiplexedWriter_->ListMultiplexedChangelogs();
        }

        virtual IChangelogPtr OpenMultiplexedChangelog(int id) override
        {
            return Impl_->MultiplexedWriter_->OpenMultiplexedChangelog(id);
        }

        virtual void MarkMultiplexedChangelogClean(int id) override
        {
            Impl_->MultiplexedWriter_->MarkMultiplexedChangelogClean(id);
        }

        virtual IChangelogPtr CreateSplitChangelog(const TChunkId& chunkId) override
        {
            auto chunkStore = Impl_->Bootstrap_->GetChunkStore();
            if (chunkStore->FindChunk(chunkId)) {
                return nullptr;
            }

            auto chunk = New<TJournalChunk>(
                Impl_->Bootstrap_,
                Impl_->Location_,
                TChunkDescriptor(chunkId));
            chunkStore->RegisterNewChunk(chunk);

            auto dispatcher = Impl_->Bootstrap_->GetJournalDispatcher();
            auto changelog = dispatcher->CreateChangelog(chunk->GetLocation(), chunkId, false)
                .Get()
                .ValueOrThrow();

            chunk->AttachChangelog(changelog);

            return changelog;
        }

        virtual IChangelogPtr OpenSplitChangelog(const TChunkId& chunkId) override
        {
            auto chunkStore = Impl_->Bootstrap_->GetChunkStore();
            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                return nullptr;
            }

            auto dispatcher = Impl_->Bootstrap_->GetJournalDispatcher();
            auto changelog = dispatcher->OpenChangelog(chunk->GetLocation(), chunkId)
                .Get()
                .ValueOrThrow();

            auto journalChunk = chunk->AsJournalChunk();
            journalChunk->AttachChangelog(changelog);

            return changelog;
        }

        virtual void FlushSplitChangelog(const TChunkId& chunkId) override
        {
            auto chunkStore = Impl_->Bootstrap_->GetChunkStore();
            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk)
                return;

            auto journalChunk = chunk->AsJournalChunk();
            auto changelog = journalChunk->GetAttachedChangelog();
            YCHECK(changelog);
            changelog->Flush()
                .Get()
                .ThrowOnError();

            journalChunk->DetachChangelog();
        }

        virtual bool RemoveSplitChangelog(const TChunkId& chunkId) override
        {
            auto chunkStore = Impl_->Bootstrap_->GetChunkStore();
            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                return false;
            }

            auto journalChunk = chunk->AsJournalChunk();
            chunkStore->UnregisterChunk(chunk);

            auto dispatcher = Impl_->Bootstrap_->GetJournalDispatcher();
            dispatcher->RemoveChangelog(journalChunk, false)
                .Get()
                .ThrowOnError();

            return true;
        }

    private:
        TImpl* const Impl_;

    };

};

////////////////////////////////////////////////////////////////////////////////

TJournalManager::TJournalManager(
    TDataNodeConfigPtr config,
    TLocation* location,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, location, bootstrap))
{ }

TJournalManager::~TJournalManager()
{ }

void TJournalManager::Initialize()
{
    Impl_->Initialize();
}

TFuture<IChangelogPtr> TJournalManager::OpenChangelog(
    const TChunkId& chunkId)
{
    return Impl_->OpenChangelog(chunkId);
}

TFuture<IChangelogPtr> TJournalManager::CreateChangelog(
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    return Impl_->CreateChangelog(chunkId, enableMultiplexing);
}

TFuture<void> TJournalManager::RemoveChangelog(
    TJournalChunkPtr chunk,
    bool enableMultiplexing)
{
    return Impl_->RemoveChangelog(chunk, enableMultiplexing);
}

TFuture<void> TJournalManager::AppendMultiplexedRecord(
    const TChunkId& chunkId,
    int recordId,
    const TSharedRef& recordData,
    TFuture<void> flushResult)
{
    return Impl_->AppendMultiplexedRecord(
        chunkId,
        recordId,
        recordData,
        std::move(flushResult));
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
