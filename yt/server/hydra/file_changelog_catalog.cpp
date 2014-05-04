#include "stdafx.h"
#include "file_changelog_catalog.h"
#include "changelog.h"
#include "file_changelog.h"
#include "changelog_catalog.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>
#include <core/misc/hash.h>
#include <core/misc/cache.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/parallel_awaiter.h>

#include <ytlib/hydra/version.h>

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;
static auto& Profiler = HydraProfiler;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TMultiplexedRecordHeader
{
    //! Changelog id and record id.
    TVersion Version;

    //! Cell id where the record belongs to.
    TCellGuid CellGuid;
};

static_assert(sizeof(TMultiplexedRecordHeader) == 24, "Binary size of TMultiplexedRecordHeader has changed.");

} // namespace NHydra
} // namespace NYT

DECLARE_PODTYPE(NYT::NHydra::TMultiplexedRecordHeader);

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogCatalog
    : public IChangelogCatalog
{
public:
    explicit TFileChangelogCatalog(TFileChangelogCatalogConfigPtr config)
        : Config(config)
        , ChangelogCache(New<TChangelogCache>(this))
    { }

    void Start()
    {
        LOG_INFO("Starting changelog catalog");

        try {
            // Initialize root directory.
            {
                ForcePath(Config->Path);
            }

            // Initialize stores.
            {
                auto entries = EnumerateDirectories(Config->Path);
                for (const auto& entry : entries) {
                    if (!entry.has_suffix(SplitSuffix))
                        continue;

                    TCellGuid cellGuid;
                    auto cellGuidStr = GetFileNameWithoutExtension(entry);
                    if (!TCellGuid::FromString(cellGuidStr, &cellGuid)) {
                        THROW_ERROR_EXCEPTION("Error parsing cell GUID %s", ~cellGuidStr.Quote());
                    }

                    LOG_INFO("Found changelog store %s", ~ToString(cellGuid));

                    auto store = New<TStore>(this, cellGuid);
                    YCHECK(StoreMap.insert(std::make_pair(cellGuid, store)).second);
                }
            }

            // Initialize and replay multiplexed changelogs.
            TMultiplexedChangelogReplay replay(this);
            {
                ForcePath(GetMultiplexedPath());
                replay.Run();
            }

            {
                int newId = replay.GetNewMultiplexedChangelogId();
                LOG_INFO("Creating new multiplexed changelog %d",
                    newId);

                MultiplexedChangelog = CreateFileChangelog(
                    GetMultiplexedChangelogPath(newId),
                    newId,
                    TChangelogCreateParams(),
                    Config->Multiplexed);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error starting changelog catalog") << ex;
        }

        LOG_INFO("Changelog catalog started");
    }

    virtual std::vector<IChangelogStorePtr> GetStores() override
    {
        TGuard<TSpinLock> guard(SpinLock);
        std::vector<IChangelogStorePtr> result;
        for (const auto& pair : StoreMap) {
            result.push_back(pair.second);
        }
        return result;
    }

    virtual IChangelogStorePtr CreateStore(const TCellGuid& cellGuid) override
    {
        auto store = New<TStore>(this, cellGuid);

        try {
            auto path = GetSplitPath(cellGuid);
            ForcePath(path);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error creating changelog store %s",
                ~ToString(cellGuid));
        }

        {
            TGuard<TSpinLock> guard(SpinLock);
            YCHECK(StoreMap.insert(std::make_pair(cellGuid, store)).second);
        }

        LOG_INFO("Created changelog store %s", ~ToString(cellGuid));

        return store;
    }

    virtual void RemoveStore(const TCellGuid& cellGuid) override
    {
        TStorePtr store;
        {
            TGuard<TSpinLock> guard(SpinLock);
            auto it = StoreMap.find(cellGuid);
            YCHECK(it != StoreMap.end());
            store = it->second;
            StoreMap.erase(it);
        }

        auto changelogs = ChangelogCache->GetAll();
        for (auto changelog : changelogs) {
            if (changelog->GetStore() == store) {
                changelog->Close();
            }
        }

        try {
            auto path = GetSplitPath(cellGuid);
            RemoveDirWithContents(path);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error removing changelog store %s",
                ~ToString(cellGuid));
        }

        LOG_INFO("Removed changelog store %s", ~ToString(cellGuid));
    }

    virtual IChangelogStorePtr FindStore(const TCellGuid& cellGuid) override
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = StoreMap.find(cellGuid);
        return it == StoreMap.end() ? nullptr : it->second;
    }

private:
    class TChangelog;
    typedef TIntrusivePtr<TChangelog> TChangelogPtr;

    class TStore;
    typedef TIntrusivePtr<TStore> TStorePtr;

    typedef std::pair<TCellGuid, int> TSplitChangelogKey;

    class TChangelog
        : public TCacheValueBase<TSplitChangelogKey, TChangelog>
        , public IChangelog
    {
    public:
        TChangelog(
            TFileChangelogCatalog* catalog,
            TStorePtr store,
            IChangelogPtr splitChangelog)
            : TCacheValueBase(std::make_pair(store->GetCellGuid(), splitChangelog->GetId()))
            , Catalog(catalog)
            , Store(store)
            , SplitChangelog(splitChangelog)
        { }

        TStorePtr GetStore() const
        {
            return Store;
        }

        virtual int GetId() const override
        {
            return SplitChangelog->GetId();
        }

        virtual int GetRecordCount() const override
        {
            return SplitChangelog->GetRecordCount();
        }

        virtual i64 GetDataSize() const override
        {
            return SplitChangelog->GetDataSize();
        }

        virtual int GetPrevRecordCount() const override
        {
            return SplitChangelog->GetPrevRecordCount();
        }

        virtual bool IsSealed() const override
        {
            return SplitChangelog->IsSealed();
        }

        virtual TFuture<void> Append(const TSharedRef& data) override
        {
            // Put the record into the split changelog.
            int recordId = SplitChangelog->GetRecordCount();
            LastAppendResult = SplitChangelog->Append(data);

            // Construct the multiplexed data record.
            TMultiplexedRecord record;
            record.Header.Version = TVersion(SplitChangelog->GetId(), recordId);
            record.Header.CellGuid = Store->GetCellGuid();
            record.Data = data;

            // Put the multiplexed record into the multiplexed changelog.
            return Catalog->Append(record, this);
        }

        virtual TFuture<void> Flush() override
        {
            return SplitChangelog->Flush();
        }

        virtual void Close() override
        {
            SplitChangelog->Close();
        }

        virtual std::vector<TSharedRef> Read(
            int firstRecordId,
            int maxRecords,
            i64 maxBytes) const override
        {
            return SplitChangelog->Read(
                firstRecordId,
                maxRecords,
                maxBytes);
        }

        virtual TFuture<void> Seal(int recordCount) override
        {
            return SplitChangelog->Seal(recordCount);
        }

        virtual void Unseal() override
        {
            SplitChangelog->Unseal();
        }

        TFuture<void> GetLastAppendResult()
        {
            return LastAppendResult;
        }

    private:
        TFileChangelogCatalog* Catalog;
        TStorePtr Store;
        IChangelogPtr SplitChangelog;

        TFuture<void> LastAppendResult;

    };

    class TStore
        : public TSizeLimitedCache<TSplitChangelogKey, TChangelog>
        , public IChangelogStore
    {
    public:
        TStore(
            TFileChangelogCatalog* catalog,
            const TCellGuid& cellGuid)
            : TSizeLimitedCache(catalog->Config->MaxCachedChangelogs)
            , Catalog(catalog)
            , CellGuid(cellGuid)
        { }

        virtual const TCellGuid& GetCellGuid() const override
        {
            return CellGuid;
        }

        virtual IChangelogPtr CreateChangelog(
            int id,
            const TChangelogCreateParams& params) override
        {
            return Catalog->ChangelogCache->CreateChangelog(
                this,
                id,
                params);
        }

        virtual IChangelogPtr TryOpenChangelog(int id) override
        {
            return Catalog->ChangelogCache->TryOpenChangelog(
                this,
                id);
        }

        virtual int GetLatestChangelogId(int initialId) override
        {
            for (int id = initialId; ; ++id) {
                auto path = Catalog->GetSplitChangelogPath(CellGuid, id);
                if (!isexist(~path)) {
                    return id == initialId ? NonexistingSegmentId : id - 1;
                }
            }
        }

    private:
        TFileChangelogCatalog* Catalog;
        TCellGuid CellGuid;

    };

    class TChangelogCache
        : public TSizeLimitedCache<TSplitChangelogKey, TChangelog>
    {
    public:
        explicit TChangelogCache(TFileChangelogCatalog* catalog)
            : TSizeLimitedCache(catalog->Config->MaxCachedChangelogs)
            , Catalog(catalog)
            , Logger(HydraLogger)
        { }

        IChangelogPtr CreateChangelog(
            TStore* store,
            int id,
            const TChangelogCreateParams& params)
        {
            auto cellGuid = store->GetCellGuid();
            TInsertCookie cookie(std::make_pair(cellGuid, id));
            if (!BeginInsert(&cookie)) {
                LOG_FATAL("Trying to create an already existing changelog %s:%d",
                    ~ToString(cellGuid),
                    id);
            }

            auto path = Catalog->GetSplitChangelogPath(cellGuid, id);

            try {
                auto splitChangelog = CreateFileChangelog(
                    path,
                    id,
                    params,
                    Catalog->Config->Split);

                auto changelog = New<TChangelog>(
                    Catalog,
                    store,
                    splitChangelog);

                cookie.EndInsert(changelog);
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Error creating changelog %s:%d",
                    ~ToString(cellGuid),
                    id);
            }

            return cookie.GetValue().Get().Value();
        }

        IChangelogPtr TryOpenChangelog(
            TStore* store,
            int id)
        {
            auto cellGuid = store->GetCellGuid();
            TInsertCookie cookie(std::make_pair(cellGuid, id));
            if (BeginInsert(&cookie)) {
                auto path = Catalog->GetSplitChangelogPath(store->GetCellGuid(), id);
                if (!isexist(~path)) {
                    cookie.Cancel(TError(
                        NHydra::EErrorCode::NoSuchChangelog,
                        "No such changelog %s:%d",
                        ~ToString(cellGuid),
                        id));
                } else {
                    try {
                        auto splitChangelog = OpenFileChangelog(
                            path,
                            id,
                            Catalog->Config->Split);

                        auto changelog = New<TChangelog>(
                            Catalog,
                            store,
                            splitChangelog);

                        cookie.EndInsert(changelog);
                    } catch (const std::exception& ex) {
                        LOG_FATAL(ex, "Error opening changelog %s:%d",
                            ~ToString(cellGuid),
                            id);
                    }
                }
            }

            auto changelogOrError = cookie.GetValue().Get();
            return changelogOrError.IsOK() ? changelogOrError.Value() : nullptr;
        }

    private:
        TFileChangelogCatalog* Catalog;
        NLog::TLogger& Logger;

    };

    class TMultiplexedChangelogReplay
    {
    public:
        explicit TMultiplexedChangelogReplay(TFileChangelogCatalog* catalog)
            : Catalog(catalog)
            , NewChangelogId(0)
        { }

        void Run()
        {
            auto entries = EnumerateFiles(Catalog->GetMultiplexedPath());

            int minId = std::numeric_limits<int>::max();
            int maxId = std::numeric_limits<int>::min();
            
            for (const auto& entry : entries) {
                if (!entry.has_suffix(LogSuffix))
                    continue;

                int id = NonexistingSegmentId;
                try {
                    id = FromString<int>(GetFileNameWithoutExtension(entry));
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

            NewChangelogId = maxId == std::numeric_limits<int>::min() ? 0 : maxId + 1;
        }

        int GetNewMultiplexedChangelogId() const
        {
            return NewChangelogId;
        }

    private:
        struct TSplitEntry
        {
            TSplitEntry()
                : RecordsAdded(0)
            { }

            explicit TSplitEntry(IChangelogPtr changelog)
                : Changelog(changelog)
                , RecordsAdded(0)
            { }

            IChangelogPtr Changelog;
            int RecordsAdded;
        };


        TFileChangelogCatalog* Catalog;

        int NewChangelogId;
        yhash_map<TSplitChangelogKey, TSplitEntry> SplitChangelogMap;


        void ReplayChangelog(int changelogId)
        {
            LOG_INFO("Replaying dirty multiplexed changelog %d", changelogId);

            auto multiplexedChangelogPath = Catalog->GetMultiplexedChangelogPath(changelogId);
            auto multiplexedChangelog = OpenFileChangelog(
                multiplexedChangelogPath,
                changelogId,
                Catalog->Config->Multiplexed);
            if (!multiplexedChangelog) {
                THROW_ERROR_EXCEPTION("Missing multiplexed changelog %d", changelogId);
            }

            int startRecordId = 0;
            int recordCount = multiplexedChangelog->GetRecordCount();
            
            if (!multiplexedChangelog->IsSealed()) {
                multiplexedChangelog->Seal(recordCount).Get();
            }
            
            while (startRecordId < recordCount) {
                auto records = multiplexedChangelog->Read(
                    startRecordId,
                    recordCount,
                    Catalog->Config->ReplayBufferSize);

                for (const auto& record : records) {
                    YCHECK(record.Size() >= sizeof (TMultiplexedRecordHeader));
                    auto* header = reinterpret_cast<const TMultiplexedRecordHeader*>(record.Begin());

                    auto* splitEntry = FindSplitEntry(header->CellGuid, header->Version.SegmentId);
                    if (!splitEntry)
                        continue;

                    if (splitEntry->Changelog->IsSealed())
                        continue;

                    int recordCount = splitEntry->Changelog->GetRecordCount();
                    if (recordCount > header->Version.RecordId)
                        continue;

                    YCHECK(recordCount == header->Version.RecordId);
                    auto splitRecord = record.Slice(TRef(
                        const_cast<char*>(record.Begin() + sizeof (TMultiplexedRecordHeader)),
                        const_cast<char*>(record.End())));
                    splitEntry->Changelog->Append(splitRecord);
                    ++splitEntry->RecordsAdded;
                }
                startRecordId += records.size();
            }

            LOG_INFO("Flushing split changelogs");

            for (auto& pair : SplitChangelogMap) {
                auto& entry = pair.second;
                
                LOG_INFO("Flushing split changelog %s:%d",
                    ~ToString(pair.first.first),
                    pair.first.second);
                entry.Changelog->Flush().Get();

                LOG_INFO("Done, %d records added",
                    entry.RecordsAdded);
                entry.RecordsAdded = 0;
            }

            Catalog->MarkMultiplexedChangelogClean(changelogId);
        }

        TSplitEntry* FindSplitEntry(const TCellGuid& cellGuid, int changelogId)
        {
            auto key = std::make_pair(cellGuid, changelogId);
            auto it = SplitChangelogMap.find(key);
            if (it == SplitChangelogMap.end()) {
                auto path = Catalog->GetSplitChangelogPath(cellGuid, changelogId);
                if (!isexist(~path)) {
                    return nullptr;
                }
                auto changelog = OpenFileChangelog(
                    path,
                    changelogId,
                    Catalog->Config->Split);
                it = SplitChangelogMap.insert(std::make_pair(
                    key,
                    TSplitEntry(changelog))).first;
            }
            return &it->second;
        }

    };

    struct TMultiplexedRecord
    {
        TMultiplexedRecordHeader Header;
        TSharedRef Data;
    };


    TFileChangelogCatalogConfigPtr Config;

    //! Protects a section of members.
    TSpinLock SpinLock;

    // ! The current multiplexed changelog.
    IChangelogPtr MultiplexedChangelog;

    //! The set of changelogs whose records were added into the current multiplexed changelog.
    //! Safeguards marking multiplexed changelogs as clean.
    yhash_set<TChangelogPtr> ActiveChangelogs;

    //! If not null then rotation is in progress; set when records in |MultiplexedBacklogQueue| are flushed.
    TPromise<void> MultiplexedBacklogPromise;
    
    //! Captures records enqueued while rotating.
    std::vector<TMultiplexedRecord> MultiplexedBacklogQueue;
    
    yhash_map<TCellGuid, TStorePtr> StoreMap;

    TIntrusivePtr<TChangelogCache> ChangelogCache;


    TFuture<void> Append(const TMultiplexedRecord& record, TChangelogPtr changelog)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);

        ActiveChangelogs.insert(changelog);

        // Check if rotation is in progress.
        if (MultiplexedBacklogPromise) {
            MultiplexedBacklogQueue.push_back(record);
            return MultiplexedBacklogPromise;
        }

        // Construct the multiplexed data record and append it.
        auto appendResult = AppendToMultiplexedChangelog(record);

        // Check if it is time to rotate.
        if (MultiplexedChangelog->GetRecordCount() >= Config->Multiplexed->RotateRecords) {
            int multiplexedChangelogId = MultiplexedChangelog->GetId();
            LOG_INFO("Started rotating multiplexed changelog %d",
                multiplexedChangelogId);
               
            auto multiplexedFlushResult = MultiplexedChangelog->Flush();

            MultiplexedBacklogPromise = NewPromise();
            YCHECK(MultiplexedBacklogQueue.empty());

            // Wait for the last record in active changelogs to get flushed
            // to mark the multiplexed changelog as clean.
            auto splitsFlushAwaiter = New<TParallelAwaiter>(GetSyncInvoker());
            for (auto changelog : ActiveChangelogs) {
                splitsFlushAwaiter->Await(changelog->GetLastAppendResult());
            }
            auto splitsFlushResult = splitsFlushAwaiter->Complete();
            ActiveChangelogs.clear();

            guard.Release();

            multiplexedFlushResult.Subscribe(
                BIND(&TFileChangelogCatalog::OnMultiplexedChangelogFlushed, MakeStrong(this))
                    .Via(GetHydraIOInvoker()));

            auto multiplexedCleanAwaiter = New<TParallelAwaiter>(GetSyncInvoker());
            multiplexedCleanAwaiter->Await(multiplexedFlushResult);
            multiplexedCleanAwaiter->Await(splitsFlushResult);
            multiplexedCleanAwaiter->Complete(BIND(
                &TFileChangelogCatalog::OnMultiplexedChangelogClean,
                MakeStrong(this),
                multiplexedChangelogId));
        }

        return appendResult;
    }

    TFuture<void> AppendToMultiplexedChangelog(const TMultiplexedRecord& record)
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

        return MultiplexedChangelog->Append(multiplexedData);
    }

    void OnMultiplexedChangelogFlushed()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MultiplexedChangelog->Seal(MultiplexedChangelog->GetRecordCount())
            .Subscribe(BIND(&TFileChangelogCatalog::OnMultiplexedChangelogSealed, MakeStrong(this))
                .Via(GetHydraIOInvoker()));
    }

    void OnMultiplexedChangelogSealed()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int oldId = MultiplexedChangelog->GetId();
        int newId = oldId + 1;

        auto newMultiplexedChangelog = CreateFileChangelog(
            GetMultiplexedChangelogPath(newId),
            newId,
            TChangelogCreateParams(),
            Config->Multiplexed);

        TGuard<TSpinLock> guard(SpinLock);

        LOG_INFO("Finished rotating multiplexed changelog %d, switching to changelog %d with a backlog of %" PRISZT " records",
            oldId,
            newId,
            MultiplexedBacklogQueue.size());

        // Deal with backlog.
        MultiplexedChangelog = newMultiplexedChangelog;

        auto appendResult = MakeFuture(); // pre-set in case MultiplexedBacklogQueue is empty
        for (const auto& record : MultiplexedBacklogQueue) {
            appendResult = AppendToMultiplexedChangelog(record);
        }
        MultiplexedBacklogQueue.clear();

        auto backlogPromise = MultiplexedBacklogPromise;
        MultiplexedBacklogPromise.Reset();

        guard.Release();

        appendResult.Subscribe(BIND([=] () mutable { backlogPromise.Set(); }));
    }

    void OnMultiplexedChangelogClean(int multiplexedChangelogId)
    {
        MarkMultiplexedChangelogClean(multiplexedChangelogId);
    }


    void MarkMultiplexedChangelogClean(int changelogId)
    {
        auto path = GetMultiplexedChangelogPath(changelogId);
        if (!Rename(path, path + CleanSuffix) ||
            !Rename(path + IndexSuffix, path + IndexSuffix + CleanSuffix))
        {
            THROW_ERROR_EXCEPTION("Error marking multiplexed changelog %s as clean",
                ~path.Quote());
        }

        LOG_INFO("Multiplexed changelog %d is clean", changelogId);
    }


    Stroka GetMultiplexedPath()
    {
        return CombinePaths(Config->Path, MultiplexedDirectory);
    }

    Stroka GetMultiplexedChangelogPath(int changelogId)
    {
        return CombinePaths(GetMultiplexedPath(), Sprintf("%09d", changelogId) + LogSuffix);
    }

    Stroka GetSplitPath(const TCellGuid& cellGuid)
    {
        return CombinePaths(Config->Path, ToString(cellGuid) + SplitSuffix);
    }

    Stroka GetSplitChangelogPath(const TCellGuid& cellGuid, int changelogId)
    {
        return CombinePaths(GetSplitPath(cellGuid), Sprintf("%09d", changelogId) + LogSuffix);
    }

};

IChangelogCatalogPtr CreateFileChangelogCatalog(
    TFileChangelogCatalogConfigPtr config)
{
    auto catalog = New<TFileChangelogCatalog>(config);
    catalog->Start();
    return catalog;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

