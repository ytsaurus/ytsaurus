#include "local_changelog_store.h"
#include "private.h"
#include "changelog.h"
#include "config.h"
#include "file_changelog_dispatcher.h"

#include <yt/ytlib/chunk_client/io_engine.h>
#include <yt/ytlib/hydra/hydra_manager.pb.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/fs.h>

namespace NYT {
namespace NHydra {

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

class TLocalChangelogStoreLock
    : public TIntrinsicRefCounted
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

private:
    std::atomic<ui64> CurrentEpoch_ = {0};

};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStoreLock)

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
        if (auto future = CheckLock()) {
            return future;
        }
        return UnderlyingChangelog_->Append(data);
    }

    virtual TFuture<void> Flush() override
    {
        if (auto future = CheckLock()) {
            return future;
        }
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
        if (auto future = CheckLock()) {
            return future;
        }
        return UnderlyingChangelog_->Truncate(recordCount);
    }

    virtual TFuture<void> Close() override
    {
        if (auto future = CheckLock()) {
            return future;
        }
        return UnderlyingChangelog_->Close();
    }

private:
    const ui64 Epoch_;
    const TLocalChangelogStoreLockPtr Lock_;
    const IChangelogPtr UnderlyingChangelog_;

    TFuture<void> CheckLock()
    {
        // TODO(sandello): Currently broken. See YT-7421.
        return Null;
        /*
        return Lock_->IsAcquired(Epoch_)
            ? Null
            : MakeFuture<void>(TError("Changelog store lock expired"));
        */
    }
};

DEFINE_REFCOUNTED_TYPE(TEpochBoundLocalChangelog)

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
        return UnderlyingChangelog_->Append(data);
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
        return UnderlyingChangelog_->Close();
    }

private:
    const IChangelogPtr UnderlyingChangelog_;

};

DEFINE_REFCOUNTED_TYPE(TCachedLocalChangelog)

class TLocalChangelogStoreFactory
    : public TAsyncSlruCacheBase<int, TCachedLocalChangelog>
    , public IChangelogStoreFactory
{
public:
    TLocalChangelogStoreFactory(
        const NChunkClient::IIOEnginePtr& ioEngine,
        TFileChangelogStoreConfigPtr config,
        const TString& threadName,
        const NProfiling::TProfiler& profiler)
        : TAsyncSlruCacheBase(config->ChangelogReaderCache)
        , IOEngine_(ioEngine)
        , Config_(config)
        , Dispatcher_(New<TFileChangelogDispatcher>(
            IOEngine_,
            Config_,
            threadName,
            profiler))
        , Logger(NLogging::TLogger(HydraLogger)
            .AddTag("Path: %v", Config_->Path))
    { }

    void Start()
    {
        LOG_DEBUG("Preparing changelog store");

        NFS::MakeDirRecursive(Config_->Path);
        NFS::CleanTempFiles(Config_->Path);
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, ui64 epoch, const TChangelogMeta& meta)
    {
        return BIND(&TLocalChangelogStoreFactory::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id, epoch, meta);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id, ui64 epoch)
    {
        return BIND(&TLocalChangelogStoreFactory::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id, epoch);
    }

    virtual TFuture<IChangelogStorePtr> Lock() override
    {
        return BIND(&TLocalChangelogStoreFactory::DoLock, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

private:
    const NChunkClient::IIOEnginePtr IOEngine_;
    const TFileChangelogStoreConfigPtr Config_;
    const TFileChangelogDispatcherPtr Dispatcher_;

    const TLocalChangelogStoreLockPtr Lock_ = New<TLocalChangelogStoreLock>();

    const NLogging::TLogger Logger;


    IChangelogPtr DoCreateChangelog(int id, ui64 epoch, const TChangelogMeta& meta)
    {
        auto cookie = BeginInsert(id);
        if (!cookie.IsActive()) {
            THROW_ERROR_EXCEPTION("Trying to create an already existing changelog %v",
                id);
        }

        auto path = GetChangelogPath(Config_->Path, id);

        try {
            auto underlyingChangelog = Dispatcher_->CreateChangelog(path, meta, Config_);
            auto cachedChangelog = New<TCachedLocalChangelog>(id, underlyingChangelog);
            cookie.EndInsert(cachedChangelog);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error creating changelog %v",
                path);
        }

        auto cachedChangelog = WaitFor(cookie.GetValue())
            .ValueOrThrow();
        return New<TEpochBoundLocalChangelog>(epoch, Lock_, std::move(cachedChangelog));
    }

    IChangelogPtr DoOpenChangelog(int id, ui64 epoch)
    {
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
                    auto underlyingChangelog = Dispatcher_->OpenChangelog(path, Config_);
                    auto cachedChangelog = New<TCachedLocalChangelog>(id, underlyingChangelog);
                    cookie.EndInsert(cachedChangelog);
                } catch (const std::exception& ex) {
                    LOG_FATAL(ex, "Error opening changelog %v",
                        path);
                }
            }
        }

        auto cachedChangelog = WaitFor(cookie.GetValue())
            .ValueOrThrow();
        return New<TEpochBoundLocalChangelog>(epoch, Lock_, std::move(cachedChangelog));
    }

    IChangelogStorePtr DoLock()
    {
        try {
            WaitFor(Dispatcher_->FlushChangelogs())
                .ThrowOnError();

            auto epoch = Lock_->Acquire();

            auto reachableVersion = ComputeReachableVersion(epoch);

            return CreateStore(reachableVersion, epoch);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error locking local changelog store %v",
                Config_->Path)
                << ex;
        }
    }

    IChangelogStorePtr CreateStore(TVersion reachableVersion, ui64 epoch);

    int GetLatestChangelogId()
    {
        int latestId = InvalidSegmentId;

        auto fileNames = NFS::EnumerateFiles(Config_->Path);
        for (const auto& fileName : fileNames) {
            auto extension = NFS::GetFileExtension(fileName);
            if (extension != ChangelogExtension)
                continue;
            auto name = NFS::GetFileNameWithoutExtension(fileName);
            try {
                int id = FromString<int>(name);
                if (id > latestId || latestId == InvalidSegmentId) {
                    latestId = id;
                }
            } catch (const std::exception&) {
                LOG_WARNING("Found unrecognized file %Qv", fileName);
            }
        }

        return latestId;
    }

    TVersion ComputeReachableVersion(ui64 epoch)
    {
        int latestId = GetLatestChangelogId();

        if (latestId == InvalidSegmentId) {
            return TVersion();
        }

        try {
            auto changelog = WaitFor(OpenChangelog(latestId, epoch))
                .ValueOrThrow();
            return TVersion(latestId, changelog->GetRecordCount());
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error opening changelog %v",
                GetChangelogPath(Config_->Path, latestId));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStoreFactory)

class TLocalChangelogStore
    : public IChangelogStore
{
public:
    TLocalChangelogStore(
        TLocalChangelogStoreFactoryPtr factory,
        ui64 epoch,
        TVersion reachableVersion)
        : Factory_(factory)
        , Epoch_(epoch)
        , ReachableVersion_(reachableVersion)
    { }

    virtual TVersion GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    virtual TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta) override
    {
        return Factory_->CreateChangelog(id, Epoch_, meta);
    }

    virtual TFuture<IChangelogPtr> OpenChangelog(int id) override
    {
        return Factory_->OpenChangelog(id, Epoch_);
    }

private:
    const TLocalChangelogStoreFactoryPtr Factory_;
    const ui64 Epoch_;
    const TVersion ReachableVersion_;

};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStore)

IChangelogStorePtr TLocalChangelogStoreFactory::CreateStore(TVersion reachableVersion, ui64 epoch)
{
    return New<TLocalChangelogStore>(this, epoch, reachableVersion);
}

IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    TFileChangelogStoreConfigPtr config,
    const TString& threadName,
    const NProfiling::TProfiler& profiler)
{
    auto store = New<TLocalChangelogStoreFactory>(
        NChunkClient::CreateIOEngine(config->IOEngineType, config->IOConfig),
        config,
        threadName,
        profiler);
    store->Start();
    return store;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

