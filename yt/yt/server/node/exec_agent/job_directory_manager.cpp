#include "job_directory_manager.h"

#include "private.h"

#include <yt/server/node/data_node/config.h>

#ifdef _linux_
#include <yt/server/lib/containers/porto_executor.h>
#endif

#include <yt/ytlib/tools/tools.h>
#include <yt/ytlib/tools/proc.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <util/string/vector.h>

namespace NYT::NExecAgent {

using namespace NConcurrency;
#ifdef _linux_
using namespace NContainers;
#endif
using namespace NDataNode;
using namespace NYTree;
using namespace NYson;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TPortoJobDirectoryManager
    : public IJobDirectoryManager
{
public:
    TPortoJobDirectoryManager(
        const TVolumeManagerConfigPtr& config,
        const TString& path,
        int locationIndex)
        : Path_(path)
        , Executor_(CreatePortoExecutor(
            config->PortoExecutor,
            Format("job_dir:%v", locationIndex),
            ExecAgentProfiler.AppendPath("/job_directory/porto")))
    {
        // Collect and drop all existing volumes.
        auto volumePaths = WaitFor(Executor_->ListVolumePaths())
            .ValueOrThrow();

        for (const auto& volumePath : volumePaths) {
            if (volumePath.StartsWith(Path_ + "/")) {
                YT_LOG_DEBUG("Unlink old volume, left from previous run (Path: %v)", volumePath);
                auto error = WaitFor(Executor_->UnlinkVolume(volumePath, "self"));
                if (error.FindMatching(EPortoErrorCode::VolumeNotFound)) {
                    YT_LOG_DEBUG(error, "Failed to unlink volume (Path: %v)", volumePath);
                } else {
                    error.ThrowOnError();
                }
            }
        }
    }

    virtual TFuture<void> ApplyQuota(const TString& path, const TJobDirectoryProperties& properties) override
    {
        return DoCreateVolume(path, properties, false);
    }

    virtual TFuture<void> CreateTmpfsDirectory(const TString& path, const TJobDirectoryProperties& properties) override
    {
        return DoCreateVolume(path, properties, true);
    }

    virtual TFuture<void> CleanDirectories(const TString& pathPrefix) override
    {
        std::vector<TString> toRelease;
        {
            auto guard = Guard(SpinLock_);
            auto it = ManagedVolumes_.lower_bound(pathPrefix);
            while (it != ManagedVolumes_.end() && (*it == pathPrefix || it->StartsWith(pathPrefix + "/"))) {
                toRelease.push_back(*it);
                it = ManagedVolumes_.erase(it);
            }
        }

        // Sort from longest paths, to shortest.
        std::sort(toRelease.begin(), toRelease.end(), [] (const TString& lhs, const TString& rhs) {
            return SplitString(lhs, "/").size() > SplitString(rhs, "/").size();
        });

        std::vector<TFuture<void>> asyncUnlinkResults;
        for (const auto& path : toRelease) {
            YT_LOG_DEBUG("Releasing porto volume (Path: %v)", path);
            try {
                // NB(psushin): it is important to clean volume contents before removal.
                // Otherwise porto can hang up in sync call for a long time during unlink of quota backend.
                RunTool<TRemoveDirContentAsRootTool>(path);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to remove directory contents for porto volume (Path: %v)", path);
            }

            asyncUnlinkResults.emplace_back(Executor_->UnlinkVolume(path, "self"));
        }

        return Combine(asyncUnlinkResults);
    }

private:
    const TString Path_;
    const IPortoExecutorPtr Executor_;

    TSpinLock SpinLock_;
    std::set<TString> ManagedVolumes_;

    TFuture<void> DoCreateVolume(const TString& path, const TJobDirectoryProperties& properties, bool isTmpfs)
    {
        THashMap<TString, TString> volumeProperties;

        if (isTmpfs) {
            volumeProperties["backend"] = "tmpfs";
        } else if (properties.DiskSpaceLimit || properties.InodeLimit) {
            volumeProperties["backend"] = "quota";
        } else {
            return VoidFuture;
        }

        volumeProperties["user"] = ToString(properties.UserId);
        volumeProperties["permissions"] = "0777";

        if (properties.DiskSpaceLimit) {
            volumeProperties["space_limit"] = ToString(*properties.DiskSpaceLimit);
        }

        if (properties.InodeLimit) {
            volumeProperties["inode_limit"] = ToString(*properties.InodeLimit);
        }

        auto onVolumeCreated = BIND([this_ = MakeStrong(this)] (const TString& volumePath) {
            auto guard = Guard(this_->SpinLock_);
            YT_VERIFY(this_->ManagedVolumes_.insert(volumePath).second);
        });

        return Executor_->CreateVolume(path, volumeProperties)
            .Apply(onVolumeCreated);
    }
};

IJobDirectoryManagerPtr CreatePortoJobDirectoryManager(
    TVolumeManagerConfigPtr config,
    const TString& path,
    int locationIndex)
{
    return New<TPortoJobDirectoryManager>(std::move(config), path, locationIndex);
}

#endif

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobDirectoryManager
    : public IJobDirectoryManager
{
public:
    TSimpleJobDirectoryManager(
        IInvokerPtr invoker,
        const TString& path,
        bool detachedTmpfsUmount)
        : Invoker_(std::move(invoker))
        , Path_(path)
        , DetachedTmpfsUmount_(detachedTmpfsUmount)
    {
        // NB: iterating over /proc/mounts is not reliable,
        // see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=593516.
        // To avoid problems with undeleting tmpfs ordered by user in sandbox
        // we always try to remove it several times.
        for (int attempt = 0; attempt < TmpfsRemoveAttemptCount; ++attempt) {
            auto mountPoints = NFS::GetMountPoints("/proc/mounts");
            for (const auto& mountPoint : mountPoints) {
                if (mountPoint.Path == Path_ || mountPoint.Path.StartsWith(Path_ + "/")) {
                    Directories_.insert(mountPoint.Path);
                }
            }

            auto error = WaitFor(CleanDirectories(Path_));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to initialize simple job directory manager")
                    << TErrorAttribute("path", Path_)
                    << error;
            }
        }
    }

    virtual TFuture<void> ApplyQuota(const TString& path, const TJobDirectoryProperties& properties) override
    {
        if (!properties.InodeLimit && !properties.DiskSpaceLimit) {
            return VoidFuture;
        }

        auto config = New<TFSQuotaConfig>();
        config->DiskSpaceLimit = properties.DiskSpaceLimit;
        config->InodeLimit = properties.InodeLimit;
        config->UserId = properties.UserId;
        config->Path = path;

        return BIND([=, this_ = MakeStrong(this)] () {
            return RunTool<TFSQuotaTool>(config);
        })
        .AsyncVia(Invoker_)
        .Run();
    }

    virtual TFuture<void> CreateTmpfsDirectory(const TString& path, const TJobDirectoryProperties& properties) override
    {
        auto config = New<TMountTmpfsConfig>();
        config->Path = path;
        config->Size = properties.DiskSpaceLimit.value_or(std::numeric_limits<i64>::max());
        config->UserId = properties.UserId;

        YT_LOG_DEBUG("Mounting tmpfs (Config: %v)",
            ConvertToYsonString(config, EYsonFormat::Text));

        return BIND([=, this_ = MakeStrong(this)] () {
            RunTool<TMountTmpfsAsRootTool>(config);
            YT_VERIFY(Directories_.insert(path).second);
        })
        .AsyncVia(Invoker_)
        .Run();
    }

    virtual TFuture<void> CleanDirectories(const TString& pathPrefix) override
    {
        return BIND([=, this_ = MakeStrong(this)] () {
            std::vector<TString> toRelease;
            auto it = Directories_.lower_bound(pathPrefix);
            while (it != Directories_.end() && (*it == pathPrefix || it->StartsWith(pathPrefix + "/"))) {
                toRelease.push_back(*it);
                it = Directories_.erase(it);
            }

            // Sort from longest paths, to shortest.
            std::sort(toRelease.begin(), toRelease.end(), [] (const TString& lhs, const TString& rhs) {
                return SplitString(lhs, "/").size() > SplitString(rhs, "/").size();
            });

            for (const auto& path : toRelease) {
                YT_LOG_DEBUG("Removing mount point (Path: %v)", path);
                try {
                    // Due to bug in the kernel, this can sometimes fail with "Directory is not empty" error.
                    // More info: https://bugzilla.redhat.com/show_bug.cgi?id=1066751
                    RunTool<TRemoveDirContentAsRootTool>(path);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to remove mount point (Path: %v)", path);
                }

                auto config = New<TUmountConfig>();
                config->Path = path;
                config->Detach = DetachedTmpfsUmount_;
                RunTool<TUmountAsRootTool>(config);
            }
        })
        .AsyncVia(Invoker_)
        .Run();
    }

private:
    const IInvokerPtr Invoker_;
    const TString Path_;
    const bool DetachedTmpfsUmount_;

    std::set<TString> Directories_;
};

IJobDirectoryManagerPtr CreateSimpleJobDirectoryManager(
    IInvokerPtr invoker,
    const TString& path,
    bool detachedTmpfsUmount)
{
    return New<TSimpleJobDirectoryManager>(
        std::move(invoker),
        path,
        detachedTmpfsUmount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
