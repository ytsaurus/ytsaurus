#include "job_directory_manager.h"

#include <yt/yt/server/node/exec_node/private.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <util/string/vector.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class THostJobDirectoryManager
    : public IJobDirectoryManager
{
public:
    THostJobDirectoryManager(
        IInvokerPtr invoker,
        const std::string& path,
        bool detachedTmpfsUmount)
        : Invoker_(std::move(invoker))
        , Path_(path)
        , DetachedTmpfsUmount_(detachedTmpfsUmount)
    {
        // NB: Iterating over /proc/mounts is not reliable,
        // see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=593516.
        // To avoid problems with undeleting tmpfs ordered by user in sandbox
        // we always try to remove it several times.
        for (int attempt = 0; attempt < TmpfsRemoveAttemptCount; ++attempt) {
            auto mountPoints = NFS::GetMountPoints("/proc/mounts");
            for (const auto& mountPoint : mountPoints) {
                if (mountPoint.Path.starts_with(Path_ + "/")) {
                    Directories_.insert(mountPoint.Path);
                }
            }

            auto error = WaitFor(CleanDirectories(Path_));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to initialize host job directory manager")
                    .With("path", Path_)
                    .With(std::move(error));
            }
        }
    }

    TFuture<void> ApplyQuota(const std::string& path, const TJobDirectoryProperties& properties) override
    {
        if (!properties.InodeLimit && !properties.DiskSpaceLimit) {
            return OKFuture;
        }

        auto config = New<TFSQuotaConfig>();
        config->DiskSpaceLimit = properties.DiskSpaceLimit;
        config->InodeLimit = properties.InodeLimit;
        config->UserId = properties.UserId;
        config->Path = path;

        return BIND([=] {
            return RunTool<TFSQuotaTool>(config);
        })
        .AsyncVia(Invoker_)
        .Run();
    }

    TFuture<void> CreateTmpfsDirectory(const std::string& path, const TJobDirectoryProperties& properties) override
    {
        auto config = New<TMountTmpfsConfig>();
        config->Path = path;
        config->Size = properties.DiskSpaceLimit.value_or(std::numeric_limits<i64>::max());
        config->UserId = properties.UserId;

        YT_TLOG_DEBUG("Mounting tmpfs")
            .With("Config", ConvertToYsonString(config, EYsonFormat::Text));

        return BIND([=, this, this_ = MakeStrong(this)] {
            RunTool<TMountTmpfsAsRootTool>(config);
            YT_VERIFY(Directories_.insert(path).second);
        })
        .AsyncVia(Invoker_)
        .Run();
    }

    TFuture<void> CleanDirectories(const std::string& pathPrefix) override
    {
        return BIND([=, this, this_ = MakeStrong(this)] {
            std::vector<std::string> toRelease;
            auto it = Directories_.lower_bound(pathPrefix);
            while (it != Directories_.end() && (*it == pathPrefix || it->starts_with(pathPrefix + "/"))) {
                toRelease.push_back(*it);
                it = Directories_.erase(it);
            }

            // Sort from longest paths, to shortest.
            std::sort(toRelease.begin(), toRelease.end(), [] (const std::string& lhs, const std::string& rhs) {
                return StringSplitter(lhs).Split('/').Count() > StringSplitter(rhs).Split('/').Count();
            });

            for (const auto& path : toRelease) {
                YT_TLOG_DEBUG("Removing mount point")
                    .With("Path", path);
                try {
                    // Due to bug in the kernel, this can sometimes fail with "Directory is not empty" error.
                    // More info: https://bugzilla.redhat.com/show_bug.cgi?id=1066751
                    RunTool<TRemoveDirContentAsRootTool>(path);
                } catch (const std::exception& ex) {
                    YT_TLOG_WARNING("Failed to remove mount point")
                        .With("Path", path)
                        .With(ex);
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

    void OnDynamicConfigChanged(const TJobDirectoryManagerDynamicConfigPtr& /*newConfig*/) override
    { }

private:
    const IInvokerPtr Invoker_;
    const std::string Path_;
    const bool DetachedTmpfsUmount_;

    std::set<std::string> Directories_;
};

IJobDirectoryManagerPtr CreateHostJobDirectoryManager(
    IInvokerPtr invoker,
    const std::string& path,
    bool detachedTmpfsUmount)
{
    return New<THostJobDirectoryManager>(
        std::move(invoker),
        path,
        detachedTmpfsUmount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
