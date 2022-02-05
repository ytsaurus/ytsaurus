#include "private.h"
#include "local_changelog_store.h"
#include "file_changelog_dispatcher.h"
#include "file_helpers.h"
#include "serialize.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core//concurrency/action_queue.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLocalChangelogStore)
DECLARE_REFCOUNTED_CLASS(TLocalChangelogStoreFactory)
DECLARE_REFCOUNTED_CLASS(TLocalChangelogStoreLock)
DECLARE_REFCOUNTED_CLASS(TCachedLocalChangelog)
DECLARE_REFCOUNTED_CLASS(TEpochBoundLocalChangelog)

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetChangelogPath(const TString& path, int id)
{
    return NFS::CombinePaths(
        path,
        Format("%09d.%v", id, ChangelogExtension));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TLocalChangelogStoreLock
    : public TRefCounted
{
public:
    ui64 Acquire()
    {
        return ++CurrentEpoch_;
    }

    bool IsAcquired(ui64 epoch) const
    {
        return CurrentEpoch_.load() == epoch;
    }

    TFuture<void> CheckAcquired(ui64 epoch) const
    {
        if (!IsAcquired(epoch)) {
            return MakeFuture<void>(MakeLockExpiredError());
        }
        return {};
    }

    void ValidateAcquire(ui64 epoch) const
    {
        if (!IsAcquired(epoch)) {
            THROW_ERROR(MakeLockExpiredError());
        }
    }

private:
    std::atomic<ui64> CurrentEpoch_ = 0;

    static TError MakeLockExpiredError()
    {
        return TError("Local changelog store lock expired");
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStoreLock)

////////////////////////////////////////////////////////////////////////////////

class TEpochBoundLocalChangelog
    : public IChangelog
{
public:
    TEpochBoundLocalChangelog(
        ui64 epoch,
        TLocalChangelogStoreLockPtr lock,
        IChangelogPtr underlyingChangelog)
        : Epoch_(epoch)
        , Lock_(std::move(lock))
        , UnderlyingChangelog_(std::move(underlyingChangelog))
    { }

    int GetId() const override
    {
        return UnderlyingChangelog_->GetId();
    }

    const NProto::TChangelogMeta& GetMeta() const override
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

    TFuture<void> Append(TRange<TSharedRef> records) override
    {
        if (auto future = CheckOpen()) {
            return future;
        }
        if (auto future = CheckLock()) {
            return future;
        }
        return UnderlyingChangelog_->Append(records);
    }

    TFuture<void> Flush() override
    {
        if (auto future = CheckOpen()) {
            return future;
        }
        if (auto future = CheckLock()) {
            return future;
        }
        return UnderlyingChangelog_->Flush();
    }

    TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        if (auto future = CheckOpen<std::vector<TSharedRef>>()) {
            return future;
        }
        return UnderlyingChangelog_->Read(firstRecordId, maxRecords, maxBytes);
    }

    TFuture<void> Truncate(int recordCount) override
    {
        if (auto future = CheckOpen()) {
            return future;
        }
        if (auto future = CheckLock()) {
            return future;
        }
        return UnderlyingChangelog_->Truncate(recordCount);
    }

    TFuture<void> Close() override
    {
        if (auto future = CheckLock()) {
            return future;
        }
        Open_ = false;
        return Flush();
    }

private:
    const ui64 Epoch_;
    const TLocalChangelogStoreLockPtr Lock_;
    const IChangelogPtr UnderlyingChangelog_;

    bool Open_ = true;


    template <class T = void>
    TFuture<T> CheckOpen() const
    {
        if (!Open_) {
            return MakeFuture<T>(TError(
                NHydra::EErrorCode::InvalidChangelogState,
                "Changelog is not open"));
        }
        return {};
    }

    TFuture<void> CheckLock() const
    {
        return Lock_->CheckAcquired(Epoch_);
    }
};

DEFINE_REFCOUNTED_TYPE(TEpochBoundLocalChangelog)

////////////////////////////////////////////////////////////////////////////////

class TCachedLocalChangelog
    : public TAsyncCacheValueBase<int, TCachedLocalChangelog>
    , public IChangelog
{
public:
    TCachedLocalChangelog(
        int id,
        IChangelogPtr underlyingChangelog)
        : TAsyncCacheValueBase(id)
        , UnderlyingChangelog_(std::move(underlyingChangelog))
    { }

    int GetId() const override
    {
        return UnderlyingChangelog_->GetId();
    }

    const NProto::TChangelogMeta& GetMeta() const override
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

    TFuture<void> Append(TRange<TSharedRef> records) override
    {
        return UnderlyingChangelog_->Append(records);
    }

    TFuture<void> Flush() override
    {
        return UnderlyingChangelog_->Flush();
    }

    TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        return UnderlyingChangelog_->Read(firstRecordId, maxRecords, maxBytes);
    }

    TFuture<void> Truncate(int recordCount) override
    {
        return UnderlyingChangelog_->Truncate(recordCount);
    }

    TFuture<void> Close() override
    {
        // TEpochBoundChangelog never propagates Close below.
        YT_ABORT();
    }

private:
    const IChangelogPtr UnderlyingChangelog_;
};

DEFINE_REFCOUNTED_TYPE(TCachedLocalChangelog)

////////////////////////////////////////////////////////////////////////////////

class TLocalChangelogStoreFactory
    : public TAsyncSlruCacheBase<int, TCachedLocalChangelog>
    , public IChangelogStoreFactory
{
public:
    TLocalChangelogStoreFactory(
        NIO::IIOEnginePtr ioEngine,
        TFileChangelogStoreConfigPtr config,
        const TString& threadName,
        const NProfiling::TProfiler& profiler)
        : TAsyncSlruCacheBase(config->ChangelogReaderCache)
        , IOEngine_(std::move(ioEngine))
        , Config_(std::move(config))
        , Dispatcher_(CreateFileChangelogDispatcher(
            IOEngine_,
            Config_,
            threadName,
            profiler))
        , Invoker_(Dispatcher_->GetInvoker())
        , BoundedConcurrencyInvoker_(CreateBoundedConcurrencyInvoker(Invoker_, 1))
        , Logger(HydraLogger.WithTag("Path: %v", Config_->Path))
    { }

    TFuture<IChangelogPtr> CreateChangelog(int id, ui64 epoch, const TChangelogMeta& meta)
    {
        return BIND(&TLocalChangelogStoreFactory::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(BoundedConcurrencyInvoker_)
            .Run(id, epoch, meta);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id, ui64 epoch)
    {
        return BIND(&TLocalChangelogStoreFactory::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(id, epoch);
    }

    TFuture<void> RemoveChangelog(int id, ui64 epoch)
    {
        return BIND(&TLocalChangelogStoreFactory::DoRemoveChangelog, MakeStrong(this))
            .AsyncVia(BoundedConcurrencyInvoker_)
            .Run(id, epoch);
    }

    TFuture<IChangelogStorePtr> Lock() override
    {
        return BIND(&TLocalChangelogStoreFactory::DoLock, MakeStrong(this))
            .AsyncVia(BoundedConcurrencyInvoker_)
            .Run();
    }

    TFuture<int> GetLatestChangelogId(ui64 epoch)
    {
        return BIND(&TLocalChangelogStoreFactory::DoGetLatestChangelogId, MakeStrong(this))
            .AsyncVia(BoundedConcurrencyInvoker_)
            .Run(epoch);
    }

private:
    const NIO::IIOEnginePtr IOEngine_;
    const TFileChangelogStoreConfigPtr Config_;
    const IFileChangelogDispatcherPtr Dispatcher_;

    const IInvokerPtr Invoker_;
    const IInvokerPtr BoundedConcurrencyInvoker_;

    const TLocalChangelogStoreLockPtr Lock_ = New<TLocalChangelogStoreLock>();

    const NLogging::TLogger Logger;

    bool Initialized_ = false;


    TFuture<IChangelogPtr> DoCreateChangelog(int id, ui64 epoch, const TChangelogMeta& meta)
    {
        Lock_->ValidateAcquire(epoch);

        auto cookie = BeginInsert(id);
        if (!cookie.IsActive()) {
            THROW_ERROR_EXCEPTION("Trying to create an already existing changelog %v",
                id);
        }

        auto path = GetChangelogPath(Config_->Path, id);

        try {
            auto underlyingChangelog = WaitFor(Dispatcher_->CreateChangelog(id, path, meta, Config_))
                .ValueOrThrow();

            YT_LOG_INFO("Local changelog created (ChangelogId: %v, Epoch: %v)",
                id,
                epoch);

            auto cachedChangelog = New<TCachedLocalChangelog>(id, underlyingChangelog);
            cookie.EndInsert(cachedChangelog);
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Error creating changelog (Path: %v)",
                path);
        }

        return MakeEpochBoundLocalChangelog(epoch, cookie.GetValue());
    }

    TFuture<IChangelogPtr> DoOpenChangelog(int id, ui64 epoch)
    {
        Lock_->ValidateAcquire(epoch);

        auto cookie = BeginInsert(id);
        if (cookie.IsActive()) {
            auto path = GetChangelogPath(Config_->Path, id);
            if (!NFS::Exists(path)) {
                cookie.Cancel(TError(
                    NHydra::EErrorCode::NoSuchChangelog,
                    "No such changelog %v",
                    id));
            } else {
                try {
                    auto underlyingChangelog = WaitFor(Dispatcher_->OpenChangelog(id, path, Config_))
                        .ValueOrThrow();

                    YT_LOG_INFO("Local changelog opened (ChangelogId: %v, Epoch: %v)",
                        id,
                        epoch);

                    auto cachedChangelog = New<TCachedLocalChangelog>(id, underlyingChangelog);
                    cookie.EndInsert(cachedChangelog);
                } catch (const std::exception& ex) {
                    YT_LOG_FATAL(ex, "Error opening changelog (Path: %v)",
                        path);
                }
            }
        }

        return MakeEpochBoundLocalChangelog(epoch, cookie.GetValue());
    }

    void DoRemoveChangelog(int id, ui64 epoch)
    {
        Lock_->ValidateAcquire(epoch);

        auto path = GetChangelogPath(Config_->Path, id);
        RemoveChangelogFiles(path);

        YT_LOG_INFO("Local changelog removed (ChangelogId: %v, Epoch: %v)",
            id,
            epoch);
    }

    IChangelogStorePtr DoLock()
    {
        try {
            if (!Initialized_) {
                WaitFor(BIND(&TLocalChangelogStoreFactory::DoInitialize, MakeStrong(this))
                    .AsyncVia(IOEngine_->GetAuxPoolInvoker())
                    .Run())
                    .ThrowOnError();
                Initialized_ = true;
            }

            YT_LOG_INFO("Locking local changelog store");

            WaitFor(Dispatcher_->FlushChangelogs())
                .ThrowOnError();

            auto epoch = Lock_->Acquire();

            auto reachableVersion = ComputeReachableVersion(epoch);
            auto electionPriority = ComputeElectionPriority(epoch);

            YT_LOG_INFO("Local changelog store locked (Epoch: %v, ReachableVersion: %v, ElectionPriority: %v)",
                epoch,
                reachableVersion,
                electionPriority);

            return CreateStore(reachableVersion, electionPriority, epoch);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error locking local changelog store %v",
                Config_->Path)
                << ex;
        }
    }

    void DoInitialize()
    {
        YT_LOG_INFO("Initializing local changelog store");

        NFS::MakeDirRecursive(Config_->Path);
        NFS::CleanTempFiles(Config_->Path);

        YT_LOG_INFO("Local changelog store initialized");
    }

    IChangelogStorePtr CreateStore(
        TVersion reachableVersion,
        TElectionPriority electionPriority,
        ui64 epoch);

    TFuture<IChangelogPtr> MakeEpochBoundLocalChangelog(ui64 epoch, const TFuture<TCachedLocalChangelogPtr>& future)
    {
        return future.Apply(BIND([epoch, lock = Lock_] (const TCachedLocalChangelogPtr& cachedChangelog) -> IChangelogPtr {
            return New<TEpochBoundLocalChangelog>(epoch, lock, cachedChangelog);
        }));
    }

    int DoGetLatestChangelogId(ui64 /*epoch*/) const
    {
        int latestId = InvalidSegmentId;

        auto fileNames = NFS::EnumerateFiles(Config_->Path);
        for (const auto& fileName : fileNames) {
            auto extension = NFS::GetFileExtension(fileName);
            if (extension != ChangelogExtension) {
                continue;
            }

            auto name = NFS::GetFileNameWithoutExtension(fileName);

            int id;
            if (!TryFromString<int>(name, id)) {
                YT_LOG_WARNING("Found unrecognized file in local changelog store (FileName: %v)",
                    fileName);
                continue;
            }

            if (id > latestId || latestId == InvalidSegmentId) {
                latestId = id;
            }
        }

        return latestId;
    }

    TVersion ComputeReachableVersion(ui64 epoch)
    {
        int latestId = DoGetLatestChangelogId(epoch);

        if (latestId == InvalidSegmentId) {
            return {};
        }

        try {
            auto changelog = WaitFor(OpenChangelog(latestId, epoch))
                .ValueOrThrow();
            return TVersion(latestId, changelog->GetRecordCount());
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Error computing reachable version for changelog (Path: %v)",
                GetChangelogPath(Config_->Path, latestId));
        }
    }

    TElectionPriority ComputeElectionPriority(ui64 epoch)
    {
        std::vector<int> ids;
        auto fileNames = NFS::EnumerateFiles(Config_->Path);
        for (const auto& fileName : fileNames) {
            auto extension = NFS::GetFileExtension(fileName);
            if (extension != ChangelogExtension) {
                continue;
            }
            auto name = NFS::GetFileNameWithoutExtension(fileName);
            try {
                int id = FromString<int>(name);
                ids.push_back(id);
            } catch (const std::exception&) {
                YT_LOG_WARNING("Found unrecognized file in local changelog store (FileName: %v)",
                    fileName);
            }
        }

        std::sort(ids.begin(), ids.end(), std::greater<int>());
        int currentTerm = -1;
        // Let's hope there are not a lot of empty changelogs.
        for (auto id : ids) {
            try {
                auto changelog = WaitFor(OpenChangelog(id, epoch))
                    .ValueOrThrow();

                if (currentTerm == -1) {
                    auto meta = changelog->GetMeta();
                    currentTerm = meta.term();
                }

                auto recordCount = changelog->GetRecordCount();
                if (recordCount == 0) {
                    YT_LOG_DEBUG("Skipping empty changelog (ChangelogId: %v)",
                        id);
                    continue;
                }

                YT_LOG_DEBUG("Nonempty changelog found (ChangelogId: %v, RecordCount: %v)",
                    id,
                    recordCount);

                auto asyncRecordsData = changelog->Read(
                    0,
                    1,
                    std::numeric_limits<i64>::max());
                auto recordsData = WaitFor(asyncRecordsData)
                    .ValueOrThrow();

                if (recordsData.empty()) {
                    THROW_ERROR_EXCEPTION("Read zero records in changelog %v",
                        id);
                }

                TMutationHeader header;
                TSharedRef requestData;
                DeserializeMutationRecord(recordsData[0], &header, &requestData);

                auto meta = changelog->GetMeta();
                YT_VERIFY(currentTerm >= 0);
                return {currentTerm, meta.term(), id, header.sequence_number() + recordCount - 1};
            } catch (const std::exception& ex) {
                YT_LOG_FATAL(ex, "Error computing election priority for changelog (Path: %v)",
                    GetChangelogPath(Config_->Path, id));
            }
        }

        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

class TLocalChangelogStore
    : public IChangelogStore
{
public:
    TLocalChangelogStore(
        TLocalChangelogStoreFactoryPtr factory,
        ui64 epoch,
        TVersion reachableVersion,
        TElectionPriority electionPriority)
        : Factory_(factory)
        , Epoch_(epoch)
        , ReachableVersion_(reachableVersion)
        , ElectionPriority_(electionPriority)
    { }

    bool IsReadOnly() const override
    {
        return false;
    }

    TFuture<int> GetLatestChangelogId() const override
    {
        return Factory_->GetLatestChangelogId(Epoch_);
    }

    std::optional<TVersion> GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    std::optional<TElectionPriority> GetElectionPriority() const override
    {
        return ElectionPriority_;
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta) override
    {
        return Factory_->CreateChangelog(id, Epoch_, meta);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id) override
    {
        return Factory_->OpenChangelog(id, Epoch_);
    }

    TFuture<void> RemoveChangelog(int id) override
    {
        return Factory_->RemoveChangelog(id, Epoch_);
    }

    void Abort() override
    { }

private:
    const TLocalChangelogStoreFactoryPtr Factory_;
    const ui64 Epoch_;
    const TVersion ReachableVersion_;
    const TElectionPriority ElectionPriority_;
};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStore)

////////////////////////////////////////////////////////////////////////////////

IChangelogStorePtr TLocalChangelogStoreFactory::CreateStore(
    TVersion reachableVersion,
    TElectionPriority electionPriority,
    ui64 epoch)
{
    return New<TLocalChangelogStore>(this, epoch, reachableVersion, electionPriority);
}

IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    TFileChangelogStoreConfigPtr config,
    const TString& threadName,
    const NProfiling::TProfiler& profiler)
{
    return New<TLocalChangelogStoreFactory>(
        CreateIOEngine(config->IOEngineType, config->IOConfig),
        config,
        threadName,
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
