#include "local_changelog_store.h"
#include "private.h"
#include "changelog.h"
#include "config.h"
#include "file_changelog_dispatcher.h"

#include <yt/ytlib/hydra/hydra_manager.pb.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/fs.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLocalChangelogStore)
DECLARE_REFCOUNTED_CLASS(TLocalChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetChangelogPath(const TString& path, int id)
{
    return NFS::CombinePaths(
        path,
        Format("%09d.%v", id, ChangelogExtension));
}

} // namespace

class TCachedLocalChangelog
    : public TAsyncCacheValueBase<int, TCachedLocalChangelog>
    , public IChangelog
{
public:
    TCachedLocalChangelog(
        int id,
        IChangelogPtr underlyingChangelog)
        : TAsyncCacheValueBase(id)
        , UnderlyingChangelog_(underlyingChangelog)
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

class TLocalChangelogStoreFactory
    : public TAsyncSlruCacheBase<int, TCachedLocalChangelog>
    , public IChangelogStoreFactory
{
public:
    TLocalChangelogStoreFactory(
        TFileChangelogStoreConfigPtr config,
        const TString& threadName,
        const NProfiling::TProfiler& profiler)
        : TAsyncSlruCacheBase(config->ChangelogReaderCache)
        , Config_(config)
        , Dispatcher_(New<TFileChangelogDispatcher>(
            Config_,
            threadName,
            profiler))
    {
        Logger.AddTag("Path: %v", Config_->Path);
    }

    void Start()
    {
        LOG_DEBUG("Preparing changelog store");

        NFS::MakeDirRecursive(Config_->Path);
        NFS::CleanTempFiles(Config_->Path);
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta)
    {
        return BIND(&TLocalChangelogStoreFactory::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id, meta);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id)
    {
        return BIND(&TLocalChangelogStoreFactory::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id);
    }

    virtual TFuture<IChangelogStorePtr> Lock() override
    {
        return BIND(&TLocalChangelogStoreFactory::DoLock, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

private:
    const TFileChangelogStoreConfigPtr Config_;
    const TFileChangelogDispatcherPtr Dispatcher_;

    NLogging::TLogger Logger = HydraLogger;


    IChangelogPtr DoCreateChangelog(int id, const TChangelogMeta& meta)
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

        return WaitFor(cookie.GetValue())
            .ValueOrThrow();
    }

    IChangelogPtr DoOpenChangelog(int id)
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

        return WaitFor(cookie.GetValue())
            .ValueOrThrow();
    }

    IChangelogStorePtr DoLock()
    {
        try {
            WaitFor(Dispatcher_->FlushChangelogs())
                .ThrowOnError();

            auto reachableVersion = ComputeReachableVersion();

            return CreateStore(reachableVersion);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error locking local changelog store %v",
                Config_->Path)
                    << ex;
        }
    }

    IChangelogStorePtr CreateStore(TVersion reachableVersion);

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

    TVersion ComputeReachableVersion()
    {
        int latestId = GetLatestChangelogId();

        if (latestId == InvalidSegmentId) {
            return TVersion();
        }

        IChangelogPtr changelog;
        try {
            changelog = WaitFor(OpenChangelog(latestId))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error opening changelog %v",
                GetChangelogPath(Config_->Path, latestId));
        }

        return TVersion(latestId, changelog->GetRecordCount());
    }

};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStoreFactory)

class TLocalChangelogStore
    : public IChangelogStore
{
public:
    TLocalChangelogStore(
        TLocalChangelogStoreFactoryPtr factory,
        TVersion reachableVersion)
        : Factory_(factory)
        , ReachableVersion_(reachableVersion)
    { }

    virtual TVersion GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    virtual TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta) override
    {
        return Factory_->CreateChangelog(id, meta);
    }

    virtual TFuture<IChangelogPtr> OpenChangelog(int id) override
    {
        return Factory_->OpenChangelog(id);
    }

private:
    const TLocalChangelogStoreFactoryPtr Factory_;
    const TVersion ReachableVersion_;

};

DEFINE_REFCOUNTED_TYPE(TLocalChangelogStore)

IChangelogStorePtr TLocalChangelogStoreFactory::CreateStore(TVersion reachableVersion)
{
    return New<TLocalChangelogStore>(this, reachableVersion);
}

IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    TFileChangelogStoreConfigPtr config,
    const TString& threadName,
    const NProfiling::TProfiler& profiler)
{
    auto store = New<TLocalChangelogStoreFactory>(config, threadName, profiler);
    store->Start();
    return store;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

