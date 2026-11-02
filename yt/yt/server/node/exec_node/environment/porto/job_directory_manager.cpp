#include "job_directory_manager.h"

#include <yt/yt/server/node/exec_node/private.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#ifdef _linux_
#include <yt/yt/library/containers/porto_executor.h>
#endif

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <util/string/vector.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NContainers;
using namespace NYTree;
using namespace NYson;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TPortoJobDirectoryManager
    : public IJobDirectoryManager
{
public:
    TPortoJobDirectoryManager(
        const TJobDirectoryManagerDynamicConfigPtr& config,
        bool enableDiskQuota,
        const std::string& path,
        int locationIndex)
        : Path_(path)
        , Executor_(CreatePortoExecutor(
            config->PortoExecutor,
            Format("jobdir%v", locationIndex),
            ExecNodeProfiler().WithPrefix("/job_directory/porto")))
        , EnableDiskQuota_(enableDiskQuota)
    {
        // Collect and drop all existing volumes.
        auto volumePaths = WaitFor(Executor_->ListVolumePaths())
            .ValueOrThrow();

        for (const auto& volumePath : volumePaths) {
            if (volumePath.starts_with(Path_ + "/")) {
                YT_TLOG_DEBUG("Unlinking old volume left from previous run")
                    .With("Path", volumePath);
                auto error = WaitFor(Executor_->UnlinkVolume(volumePath, "self"));
                if (error.FindMatching(EPortoErrorCode::VolumeNotFound) || error.FindMatching(EPortoErrorCode::VolumeNotLinked)) {
                    YT_TLOG_DEBUG("Failed to unlink volume")
                        .With("Path", volumePath)
                        .With(error);
                } else {
                    error.ThrowOnError();
                }
            }
        }
    }

    TFuture<void> ApplyQuota(const std::string& path, const TJobDirectoryProperties& properties) override
    {
        if (!EnableDiskQuota_) {
            return OKFuture;
        }
        return DoCreateVolume(path, properties, false);
    }

    TFuture<void> CreateTmpfsDirectory(const std::string& path, const TJobDirectoryProperties& properties) override
    {
        return DoCreateVolume(path, properties, true);
    }

    void OnDynamicConfigChanged(const TJobDirectoryManagerDynamicConfigPtr& newConfig) override
    {
        Executor_->OnDynamicConfigChanged(newConfig->PortoExecutor);
    }

    TFuture<void> CleanDirectories(const std::string& pathPrefix) override
    {
        std::vector<std::string> toRelease;
        {
            auto guard = Guard(SpinLock_);
            auto it = ManagedVolumes_.lower_bound(pathPrefix);
            while (it != ManagedVolumes_.end() && (*it == pathPrefix || it->starts_with(pathPrefix + "/"))) {
                toRelease.push_back(*it);
                it = ManagedVolumes_.erase(it);
            }
        }

        // Sort from longest paths, to shortest.
        std::sort(toRelease.begin(), toRelease.end(), [] (const std::string& lhs, const std::string& rhs) {
            return StringSplitter(lhs).Split('/').Count() > StringSplitter(rhs).Split('/').Count();
        });

        std::vector<TFuture<void>> asyncUnlinkResults;
        for (const auto& path : toRelease) {
            YT_TLOG_DEBUG("Releasing Porto volume")
                .With("Path", path);
            try {
                // NB(psushin): it is important to clean volume contents before removal.
                // Otherwise Porto can hang up in sync call for a long time during unlink of quota backend.
                RunTool<TRemoveDirContentAsRootTool>(path);
            } catch (const std::exception& ex) {
                YT_TLOG_WARNING("Failed to remove directory contents for Porto volume")
                    .With("Path", path)
                    .With(ex);
            }

            asyncUnlinkResults.emplace_back(Executor_->UnlinkVolume(path, "self"));
        }

        return AllSucceeded(asyncUnlinkResults);
    }

private:
    const std::string Path_;
    const IPortoExecutorPtr Executor_;
    const bool EnableDiskQuota_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    std::set<std::string> ManagedVolumes_;

    TFuture<void> DoCreateVolume(const std::string& path, const TJobDirectoryProperties& properties, bool isTmpfs)
    {
        THashMap<std::string, std::string> volumeProperties;

        if (isTmpfs) {
            volumeProperties["backend"] = "tmpfs";
        } else if (properties.DiskSpaceLimit || properties.InodeLimit) {
            volumeProperties["backend"] = "quota";
        } else {
            return OKFuture;
        }

        volumeProperties["user"] = ToString(properties.UserId);
        volumeProperties["permissions"] = "0777";

        if (properties.DiskSpaceLimit) {
            volumeProperties["space_limit"] = ToString(*properties.DiskSpaceLimit);
        }

        if (properties.InodeLimit) {
            volumeProperties["inode_limit"] = ToString(*properties.InodeLimit);
        }

        auto onVolumeCreated = BIND([this, this_ = MakeStrong(this)] (const std::string& volumePath) {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(ManagedVolumes_.insert(volumePath).second);
        });

        return Executor_->CreateVolume(path, volumeProperties)
            .Apply(onVolumeCreated);
    }
};

IJobDirectoryManagerPtr CreatePortoJobDirectoryManager(
    TJobDirectoryManagerDynamicConfigPtr config,
    bool enableDiskQuota,
    const std::string& path,
    int locationIndex)
{
    return New<TPortoJobDirectoryManager>(std::move(config), enableDiskQuota, path, locationIndex);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
