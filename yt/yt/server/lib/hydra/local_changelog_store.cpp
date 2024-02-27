#include "local_changelog_store.h"

#include "private.h"
#include "file_changelog.h"
#include "file_changelog_dispatcher.h"
#include "file_helpers.h"
#include "changelog_store_helpers.h"
#include "config.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core//concurrency/action_queue.h>

#include <util/string/strip.h>

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

TString GetTermFileName(const TString& path)
{
    return NFS::CombinePaths(
        path,
        TermFileName);
}

TString GetLockFileName(const TString& path)
{
    return NFS::CombinePaths(
        path,
        LockFileName);
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

    i64 EstimateChangelogSize(i64 payloadSize) const override
    {
        return UnderlyingChangelog_->EstimateChangelogSize(payloadSize);
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
        if (CloseFuture_) {
            // If already closing, wait for the previous attempt to finish.
            return CloseFuture_;
        }
        CloseFuture_ = Flush();
        Open_ = false;
        return CloseFuture_;
    }

private:
    const ui64 Epoch_;
    const TLocalChangelogStoreLockPtr Lock_;
    const IChangelogPtr UnderlyingChangelog_;

    bool Open_ = true;
    TFuture<void> CloseFuture_;


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

    i64 EstimateChangelogSize(i64 payloadSize) const override
    {
        return UnderlyingChangelog_->EstimateChangelogSize(payloadSize);
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
            /*memoryUsageTracker*/ nullptr,
            Config_,
            threadName,
            profiler))
        , Invoker_(Dispatcher_->GetInvoker())
        , BoundedConcurrencyInvoker_(CreateBoundedConcurrencyInvoker(Invoker_, 1))
        , Logger(HydraLogger.WithTag("Path: %v", Config_->Path))
    { }

    TFuture<void> WriteTerm(int term)
    {
        return BIND(&TLocalChangelogStoreFactory::DoWriteTerm, MakeStrong(this))
            .AsyncVia(BoundedConcurrencyInvoker_)
            .Run(term);
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta, const TChangelogOptions& options, ui64 epoch)
    {
        return BIND(&TLocalChangelogStoreFactory::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(BoundedConcurrencyInvoker_)
            .Run(id, meta, options, epoch);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id, const TChangelogOptions& options, ui64 epoch)
    {
        return BIND(&TLocalChangelogStoreFactory::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(id, options, epoch);
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

private:
    const NIO::IIOEnginePtr IOEngine_;
    const TFileChangelogStoreConfigPtr Config_;
    const IFileChangelogDispatcherPtr Dispatcher_;

    const IInvokerPtr Invoker_;
    const IInvokerPtr BoundedConcurrencyInvoker_;

    const TLocalChangelogStoreLockPtr Lock_ = New<TLocalChangelogStoreLock>();

    const NLogging::TLogger Logger;

    bool Initialized_ = false;
    std::optional<TFileHandle> LockFileHandle_;


    void DoWriteTerm(int term)
    {
        WaitFor(BIND(&TLocalChangelogStoreFactory::WriteTermImpl, MakeStrong(this))
            .AsyncVia(IOEngine_->GetAuxPoolInvoker())
            .Run(term))
            .ThrowOnError();
    }

    TFuture<IChangelogPtr> DoCreateChangelog(int id, const TChangelogMeta& meta, const TChangelogOptions& /*options*/, ui64 epoch)
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

    TFuture<IChangelogPtr> DoOpenChangelog( int id, const TChangelogOptions& /*options*/, ui64 epoch)
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
                WaitFor(BIND(&TLocalChangelogStoreFactory::InitializeImpl, MakeStrong(this))
                    .AsyncVia(IOEngine_->GetAuxPoolInvoker())
                    .Run())
                    .ThrowOnError();
                Initialized_ = true;
            }

            YT_LOG_INFO("Locking local changelog store");

            WaitFor(Dispatcher_->FlushChangelogs())
                .ThrowOnError();

            auto epoch = Lock_->Acquire();

            int term = WaitFor(BIND(&TLocalChangelogStoreFactory::ReadTermImpl, MakeStrong(this))
                .AsyncVia(IOEngine_->GetAuxPoolInvoker())
                .Run())
                .ValueOrThrow();

            auto scanResult = Scan(epoch);
            auto reachableVersion = TVersion(
                scanResult.LatestChangelogId,
                scanResult.LatestChangelogRecordCount);
            auto electionPriority = TElectionPriority(
                scanResult.LastMutationTerm,
                scanResult.LatestNonemptyChangelogId,
                scanResult.LastMutationSequenceNumber);

            YT_LOG_INFO("Local changelog store locked (Epoch: %v, ReachableVersion: %v, ElectionPriority: %v, Term: %v)",
                epoch,
                reachableVersion,
                electionPriority,
                term);

            return CreateStore(
                reachableVersion,
                electionPriority,
                term,
                scanResult.LatestChangelogId,
                epoch);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error locking local changelog store %v",
                Config_->Path)
                << ex;
        }
    }


    void InitializeImpl()
    {
        YT_LOG_INFO("Initializing local changelog store");

        NFS::MakeDirRecursive(Config_->Path);
        AcquireLock();
        NFS::CleanTempFiles(Config_->Path);

        YT_LOG_INFO("Local changelog store initialized");
    }

    IChangelogStorePtr CreateStore(
        TVersion reachableVersion,
        TElectionPriority electionPriority,
        int term,
        int latestChangelogId,
        ui64 epoch);

    TFuture<IChangelogPtr> MakeEpochBoundLocalChangelog(ui64 epoch, const TFuture<TCachedLocalChangelogPtr>& future)
    {
        return future.Apply(BIND([epoch, lock = Lock_] (const TCachedLocalChangelogPtr& cachedChangelog) -> IChangelogPtr {
            return New<TEpochBoundLocalChangelog>(epoch, lock, cachedChangelog);
        }));
    }

    TChangelogStoreScanResult Scan(ui64 epoch)
    {
        std::vector<int> changelogIds;
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

            changelogIds.push_back(id);
        }

        auto recordCountGetter = [&] (int changelogId) {
            auto changelog = WaitFor(OpenChangelog(changelogId, /*options*/ {}, epoch))
                .ValueOrThrow();
            return TChangelogScanInfo{
                .RecordCount = changelog->GetRecordCount(),
            };
        };

        auto recordReader = [&] (int changelogId, int recordId, bool /*atPrimaryPath*/) {
            auto changelog = WaitFor(OpenChangelog(changelogId, /*options*/ {}, epoch))
                .ValueOrThrow();

            auto recordsData = WaitFor(changelog->Read(recordId, 1, std::numeric_limits<i64>::max()))
                .ValueOrThrow();

            if (recordsData.empty()) {
                THROW_ERROR_EXCEPTION("Unable to read record %v in changelog %v",
                    recordId,
                    changelogId);
            }

            return recordsData[0];
        };

        return ScanChangelogStore(
            changelogIds,
            recordCountGetter,
            recordReader);
    }

    void AcquireLock()
    {
        auto fileName = GetLockFileName(Config_->Path);
        LockFileHandle_.emplace(fileName, RdWr | CreateAlways);
        if (LockFileHandle_->Flock(LOCK_EX | LOCK_NB) != 0) {
            YT_LOG_FATAL("Cannot lock local changelog store");
        }
    }

    int ReadTermImpl()
    {
        auto fileName = GetTermFileName(Config_->Path);

        try {
            if (!NFS::Exists(fileName)) {
                WriteTermImpl(0);
                return 0;
            }

            auto termString = TUnbufferedFileInput(fileName).ReadAll();
            auto strippedTermString = StripString(termString, [] (const char* ch) {
                return *ch == '\n' || *ch == ' ';
            });
            return FromString<int>(strippedTermString);
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Error reading term from local changelog store (Path: %v)",
                Config_->Path);
        }
    }

    void WriteTermImpl(int term)
    {
        auto fileName = GetTermFileName(Config_->Path);
        auto tempFileName = GetTermFileName(Config_->Path) + NFS::TempFileSuffix;

        try {
            auto termString = Format("%v\n", term);

            {
                TUnbufferedFileOutput output(tempFileName);
                output.Write(termString);
                output.Flush();
            }

            NFS::Rename(tempFileName, fileName);
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Error writing term to local changelog store (Path: %v)",
                Config_->Path);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

class TLocalChangelogStore
    : public IChangelogStore
{
public:
    TLocalChangelogStore(
        NLogging::TLogger logger,
        TLocalChangelogStoreFactoryPtr factory,
        ui64 epoch,
        TVersion reachableVersion,
        TElectionPriority electionPriority,
        int term,
        int latestChangelogId)
        : Logger(std::move(logger))
        , Factory_(factory)
        , Epoch_(epoch)
        , ReachableVersion_(reachableVersion)
        , ElectionPriority_(electionPriority)
        , Term_(term)
        , LatestChangelogId_(latestChangelogId)
    { }

    bool IsReadOnly() const override
    {
        return false;
    }

    std::optional<int> TryGetTerm() const override
    {
        return Term_.load();
    }

    TFuture<void> SetTerm(int term) override
    {
        return Factory_->WriteTerm(term)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] {
                Term_.store(term);
            }));
    }

    std::optional<int> TryGetLatestChangelogId() const override
    {
        return LatestChangelogId_.load();
    }

    std::optional<TVersion> GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    std::optional<TElectionPriority> GetElectionPriority() const override
    {
        return ElectionPriority_;
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta, const TChangelogOptions& options) override
    {
        return Factory_->CreateChangelog(id, meta, options, Epoch_)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IChangelogPtr& changelog) {
                UpdateLatestChangelogId(id);
                return changelog;
            }));
    }

    TFuture<IChangelogPtr> OpenChangelog(int id, const TChangelogOptions& options) override
    {
        return Factory_->OpenChangelog(id, options, Epoch_);
    }

    TFuture<void> RemoveChangelog(int id) override
    {
        return Factory_->RemoveChangelog(id, Epoch_);
    }

    void Abort() override
    { }

private:
    const NLogging::TLogger Logger;
    const TLocalChangelogStoreFactoryPtr Factory_;
    const ui64 Epoch_;
    const TVersion ReachableVersion_;
    const TElectionPriority ElectionPriority_;

    std::atomic<int> Term_;
    std::atomic<int> LatestChangelogId_;


    void UpdateLatestChangelogId(int id)
    {
        auto expected = LatestChangelogId_.load();
        do {
            if (expected >= id) {
                return;
            }
        } while (!LatestChangelogId_.compare_exchange_weak(expected, id));
        YT_LOG_INFO("Latest changelog id updated (NewChangelogId: %v)",
            id);
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStore)

////////////////////////////////////////////////////////////////////////////////

IChangelogStorePtr TLocalChangelogStoreFactory::CreateStore(
    TVersion reachableVersion,
    TElectionPriority electionPriority,
    int term,
    int latestChangelogId,
    ui64 epoch)
{
    return New<TLocalChangelogStore>(
        Logger,
        this,
        epoch,
        reachableVersion,
        electionPriority,
        term,
        latestChangelogId);
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
