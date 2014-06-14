#include "stdafx.h"
#include "journal_dispatcher.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"
#include "session.h"
#include "chunk_store.h"
#include "journal_chunk.h"

#include <core/misc/cache.h>
#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/parallel_awaiter.h>

#include <server/hydra/private.h>
#include <server/hydra/changelog.h>
#include <server/hydra/file_changelog.h>
#include <server/hydra/lazy_changelog.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NHydra;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

static const Stroka CleanSuffix(".clean");

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

class TJournalDispatcher::TImpl
    : public TSizeLimitedCache<TChunkId, TCachedChangelog>
{
public:
    explicit TImpl(
        NCellNode::TBootstrap* bootstrap,
        TDataNodeConfigPtr config)
        : TSizeLimitedCache<TChunkId, TCachedChangelog>(config->ChangelogReaderCacheSize)
        , Bootstrap_(bootstrap)
        , Config_(config)
        , ChangelogDispatcher_(New<TFileChangelogDispatcher>("JournalFlush"))
    { }

    void Initialize();

    bool AcceptsChunks() const
    {
        return Config_->MultiplexedChangelog != nullptr;
    }

    IChangelogPtr OpenChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    IChangelogPtr CreateChangelog(
        IChunkPtr chunk,
        bool enableMultiplexing);

    TAsyncError RemoveChangelog(IChunkPtr chunk)
    {
        TMultiplexedRecord record;
        record.Header.Type = EMultiplexedRecordType::Remove;
        record.Header.ChunkId = chunk->GetId();
        record.Header.RecordId = -1;
        AppendMultiplexedRecord(record, nullptr);

        return BIND(&TImpl::DoRemoveChangelog, MakeStrong(this), chunk)
            .Guarded()
            .AsyncVia(chunk->GetLocation()->GetWriteInvoker())
            .Run();
    }

    void EvictChangelog(IChunkPtr chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }

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
    yhash_set<TCachedChangelogPtr> ActiveChangelogs_;


    IChangelogPtr DoCreateChangelog(IChunkPtr chunk)
    {
        const auto& chunkId = chunk->GetId();
        auto location = chunk->GetLocation();

        LOG_DEBUG("Started creating journal chunk (LocationId: %s, ChunkId: %s)",
            ~location->GetId(),
            ~ToString(chunkId));

        IChangelogPtr changelog;

        auto& Profiler = location->Profiler();
        PROFILE_TIMING("/journal_chunk_create_time") {
            try {
                auto fileName = location->GetChunkFileName(chunkId);
                changelog = ChangelogDispatcher_->CreateChangelog(
                    fileName,
                    TSharedRef(),
                    Config_->SplitChangelog);
            } catch (const std::exception& ex) {
                location->Disable();
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IOError,
                    "Error creating journal chunk %s",
                    ~ToString(chunkId))
                    << ex;
            }
        }

        LOG_DEBUG("Finished creating journal chunk (LocationId: %s, ChunkId: %s)",
            ~location->GetId(),
            ~ToString(chunkId));

        return changelog;
    }

    void DoRemoveChangelog(IChunkPtr chunk)
    {
        const auto& chunkId = chunk->GetId();

        LOG_DEBUG("Started removing journal chunk (ChunkId: %s)",
            ~ToString(chunkId));

        auto location = chunk->GetLocation();
        auto& Profiler = location->Profiler();
        PROFILE_TIMING("/journal_chunk_remove_time") {
            try {
                auto dataFileName = chunk->GetFileName();
                auto indexFileName = dataFileName + IndexSuffix;
                NFS::Remove(dataFileName);
                NFS::Remove(indexFileName);
            } catch (const std::exception& ex) {
                location->Disable();
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IOError,
                    "Error removing journal chunk %s",
                    ~ToString(chunkId))
                    << ex;
            }
        }

        LOG_DEBUG("Finished removing journal chunk (ChunkId: %s)",
            ~ToString(chunkId));
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

        auto sealResult = MultiplexedChangelog_->Seal(MultiplexedChangelog_->GetRecordCount());
        auto sealError = WaitFor(sealResult);
        if (!sealError.IsOK()) {
            LOG_FATAL(sealError);
        }

        LOG_INFO("Finished rotating multiplexed changelog %d",
            oldId);

        return CreateMultiplexedChangelog(newId);
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
        return NFS::CombinePaths(GetMultiplexedPath(), Sprintf("%09d", changelogId) + LogSuffix);
    }


    void MarkMultiplexedChangelogClean(int changelogId)
    {
        auto path = GetMultiplexedChangelogPath(changelogId);
        NFS::Rename(path, path + CleanSuffix);
        NFS::Rename(path + IndexSuffix, path + IndexSuffix + CleanSuffix);
        LOG_INFO("Multiplexed changelog %d is clean", changelogId);
    }

    IChangelogPtr CreateMultiplexedChangelog(int id)
    {
        LOG_INFO("Started creating new multiplexed changelog %d",
            id);

        auto changelog = ChangelogDispatcher_->CreateChangelog(
            GetMultiplexedChangelogPath(id),
            TSharedRef(),
            Config_->MultiplexedChangelog);

        LOG_INFO("Finished creating new multiplexed changelog %d",
            id);

        return changelog;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TJournalDispatcher::TCachedChangelog
    : public TCacheValueBase<TChunkId, TCachedChangelog>
    , public IChangelog
{
public:
    TCachedChangelog(
        TImplPtr owner,
        const TChunkId& chunkId,
        IChangelogPtr underlyingChangelog,
        bool enableMultiplexing)
        : TCacheValueBase<TChunkId, TCachedChangelog>(chunkId)
        , Owner_(owner)
        , UnderlyingChangelog_(underlyingChangelog)
        , EnableMultiplexing_(enableMultiplexing)
    { }

    virtual TSharedRef GetMeta() const override
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
            LastAppendResult_ = UnderlyingChangelog_->Append(data);

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

    virtual void Close() override
    {
        UnderlyingChangelog_->Close();
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

    virtual void Unseal() override
    {
        UnderlyingChangelog_->Unseal();
    }

    TAsyncError GetLastAppendResult()
    {
        YASSERT(EnableMultiplexing_);
        YASSERT(LastAppendResult_);
        return LastAppendResult_;
    }

private:
    TImplPtr Owner_;
    IChangelogPtr UnderlyingChangelog_;
    bool EnableMultiplexing_;

    TAsyncError LastAppendResult_;

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

        auto entries = NFS::EnumerateFiles(path);

        int minId = std::numeric_limits<int>::max();
        int maxId = std::numeric_limits<int>::min();
            
        for (const auto& entry : entries) {
            if (!entry.has_suffix(LogSuffix))
                continue;

            int id = NonexistingSegmentId;
            try {
                id = FromString<int>(NFS::GetFileNameWithoutExtension(entry));
            } catch (const std::exception) {
                THROW_ERROR_EXCEPTION("Error parsing multiplexed changelog id %s", ~entry.Quote());
            }

            LOG_INFO("Found dirty multiplexed changelog %d", id);

            minId = std::min(minId, id);
            maxId = std::max(maxId, id);
        }

        for (int id = minId; id <= maxId; ++id) {
            ReplayChangelog(id);
        }

        for (auto& pair : SplitMap_) {
            auto& entry = pair.second;
            entry.Chunk->DetachChangelog();
        }

        return maxId == std::numeric_limits<int>::min() ? 0 : maxId + 1;
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
        LOG_INFO("Replaying dirty multiplexed changelog %d", changelogId);

        auto multiplexedChangelogPath = Owner_->GetMultiplexedChangelogPath(changelogId);
        auto multiplexedChangelog = Owner_->ChangelogDispatcher_->OpenChangelog(
            multiplexedChangelogPath,
            Owner_->Config_->MultiplexedChangelog);

        int startRecordId = 0;
        int recordCount = multiplexedChangelog->GetRecordCount();
            
        if (!multiplexedChangelog->IsSealed()) {
            multiplexedChangelog->Seal(recordCount).Get();
        }
            
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
                
            LOG_INFO("Started flushing journal chunk (ChunkId: %s)",
                ~ToString(pair.first));

            entry.Changelog->Flush().Get();

            LOG_INFO("Finished flushing journal chunk (ChunkId: %s, RecordAdded: %d)",
                ~ToString(pair.first),
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
            chunkId,
            TChunkInfo());
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

        Owner_->DoRemoveChangelog(chunk);
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
            auto fileName = location->GetChunkFileName(chunkId);
            auto changelog = Owner_->ChangelogDispatcher_->OpenChangelog(
                fileName,
                Owner_->Config_->SplitChangelog);
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
        THROW_ERROR_EXCEPTION("Error starting journal dispatcher") << ex;
    }

    LOG_INFO("Journal dispatcher started");
}

IChangelogPtr TJournalDispatcher::TImpl::OpenChangelog(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    auto& Profiler = location->Profiler();

    TInsertCookie cookie(chunkId);
    if (BeginInsert(&cookie)) {
        auto fileName = location->GetChunkFileName(chunkId);
        LOG_DEBUG("Started opening journal chunk (LocationId: %s, ChunkId: %s)",
            ~location->GetId(),
            ~ToString(chunkId));

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

IChangelogPtr TJournalDispatcher::TImpl::CreateChangelog(
    IChunkPtr chunk,
    bool enableMultiplexing)
{
    if (!AcceptsChunks()) {
        THROW_ERROR_EXCEPTION("No new journal chunks are accepted");
    }

    const auto& chunkId = chunk->GetId();
    auto location = chunk->GetLocation();

    TInsertCookie cookie(chunkId);
    if (BeginInsert(&cookie)) {
        auto futureChangelogOrError = BIND(&TImpl::DoCreateChangelog, MakeStrong(this), chunk)
            .Guarded()
            .AsyncVia(location->GetWriteInvoker())
            .Run();
        auto lazyChangelog = CreateLazyChangelog(futureChangelogOrError);
        auto cachedChangelog = New<TCachedChangelog>(
            this,
            chunkId,
            lazyChangelog,
            enableMultiplexing);
        cookie.EndInsert(cachedChangelog);
    }

    auto cachedChangelog = cookie.GetValue().Get().Value();

    TMultiplexedRecord record;
    record.Header.Type = EMultiplexedRecordType::Create;
    record.Header.ChunkId = chunkId;
    record.Header.RecordId = -1;
    AppendMultiplexedRecord(record, nullptr);

    return cachedChangelog;
}

TAsyncError TJournalDispatcher::TImpl::AppendMultiplexedRecord(
    const TMultiplexedRecord& record,
    TCachedChangelogPtr changelog)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (changelog) {
        ActiveChangelogs_.insert(changelog);
    }

    // Construct the multiplexed data record and append it.
    auto appendResult = DoAppendMultiplexedRecord(record);

    // Check if it is time to rotate.
    const auto& config = Config_->MultiplexedChangelog;
    if (MultiplexedChangelog_->GetRecordCount() >= config->MaxRecordCount ||
        MultiplexedChangelog_->GetDataSize() >= config->MaxDataSize)
    {
        LOG_INFO("Started rotating multiplexed changelog %d",
            MultiplexedChangelogId_);

        auto multiplexedFlushResult = MultiplexedChangelog_->Flush();

        // To mark the multiplexed changelog as clean we wait for
        // * the multiplexed changelog to get flushed
        // * last appended records in all active changelogs to get flushed
        std::vector<TAsyncError> cleanResults;
        cleanResults.push_back(multiplexedFlushResult);
        for (auto changelog : ActiveChangelogs_) {
            cleanResults.push_back(changelog->GetLastAppendResult());
        }
        ActiveChangelogs_.clear();

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

IChangelogPtr TJournalDispatcher::OpenChangelog(
    TLocationPtr location,
    const TChunkId& chunkId,
    bool enableMultiplexing)
{
    return Impl_->OpenChangelog(location, chunkId, enableMultiplexing);
}

IChangelogPtr TJournalDispatcher::CreateChangelog(
    IChunkPtr chunk,
    bool enableMultiplexing)
{
    return Impl_->CreateChangelog(chunk, enableMultiplexing);
}

TAsyncError TJournalDispatcher::RemoveChangelog(IChunkPtr chunk)
{
    return Impl_->RemoveChangelog(chunk);
}

void TJournalDispatcher::EvictChangelog(IChunkPtr chunk)
{
    Impl_->EvictChangelog(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
