#include "volume_manager.h"

#include "tmpfs_volume.h"

#include <yt/yt/server/node/exec_node/helpers.h>
#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/volume_counters.h>
#include <yt/yt/server/node/exec_node/volume_options.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/misc/fs.h>

#include <util/string/vector.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THostVolumeManager)

class THostVolumeManager
    : public IHostVolumeManager
{
public:
    THostVolumeManager(
        IInvokerPtr invoker,
        bool detachedTmpfsUmount)
        : Invoker_(std::move(invoker))
        , DetachUnmount_(detachedTmpfsUmount)
    { }

    TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        const std::optional<std::string>& sandboxPath,
        const TJobId& jobId,
        const std::vector<TBaseVolumeParamsPtr>& volumes,
        const std::vector<TVolumeMountPtr>& volumeMounts) override
    {
        YT_VERIFY(sandboxPath);
        // Create debug tag.
        auto tag = TGuid::Create();

        auto future = OKFuture;
        auto result = std::make_shared<std::vector<TVolumeResultPtr>>();
        result->reserve(volumes.size());
        for (const auto& volume : volumes) {
            YT_VERIFY(volume->VolumeType == EVolumeType::Tmpfs);
            auto mountPath(GetVolumeMountPathByVolumeId(volume->VolumeId, volumeMounts));
            future = future
                .Apply(BIND([tag, jobId, sandboxPath, volume, mountPath, result, this, this_ = MakeStrong(this)] {
                    return CreateTmpfsVolume(tag, jobId, *sandboxPath, StaticPointerCast<TTmpfsVolumeParams>(volume), mountPath)
                        .Apply(
                            BIND([result = std::move(result)] (TVolumeResultPtr volumeResult) {
                                result->push_back(std::move(volumeResult));
                            })
                            .AsyncVia(GetCurrentInvoker())
                        )
                        .ToUncancelable();
                }));
        }

        return future.Apply(BIND([result] { return std::move(*result); }));
    }

    void InitializeSlot(TNonNullPtr<TSlotLocation> /*location*/, int /*slotIndex*/) override
    { }

    void PrepareForReplacement() const override
    { }

    TFuture<void> Initialize(const std::vector<TSlotLocationConfigPtr>& locations)
    {
        // NB: Iterating over /proc/mounts is not reliable,
        // see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=593516.
        // To avoid problems with undeleting tmpfs ordered by user in sandbox
        // we always try to remove it several times.
        for (int attempt = 0; attempt < TmpfsRemoveAttemptCount; ++attempt) {
            std::vector<std::string> mountPaths;
            for (const auto& location : locations) {
                FindTmpfsMountPathsInLocation(location->Path, mountPaths);
            }

            // Sort from longest paths, to shortest.
            std::sort(mountPaths.begin(), mountPaths.end(), [] (const std::string& lhs, const std::string& rhs) {
                return StringSplitter(lhs).Split('/').Count() > StringSplitter(rhs).Split('/').Count();
            });

            auto error = WaitFor(CleanupTmpfsMountPaths(std::move(mountPaths)));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to initialize host volume manager")
                    .With(error);
            }
        }

        return OKFuture;
    }

    void BuildOrchid(NYTree::TFluentAny fluent) const override
    {
        fluent
            .BeginMap()
            .EndMap();
    }

    TFuture<void> GetVolumeReleaseEvent() override
    {
        return OKFuture;
    }

    TFuture<void> Disable(const TError&) override
    {
        return OKFuture;
    }

    TError GetHealthCheckError() const override
    {
        return {};
    }

    bool IsEnabled() const override
    {
        return true;
    }

    void OnDynamicConfigChanged(
        const TVolumeManagerDynamicConfigPtr&,
        const TVolumeManagerDynamicConfigPtr&) override
    { }

private:
    const IInvokerPtr Invoker_;
    const bool DetachUnmount_;

    TFuture<TVolumeResultPtr> CreateTmpfsVolume(
        TGuid tag,
        const TJobId& jobId,
        const std::string& sandboxPath,
        const TTmpfsVolumeParamsPtr& volume,
        const TAbsoluteNormalizedPath& mountPath)
    {
        YT_VERIFY(!sandboxPath.empty());

        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "tmpfs",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        // mountPath is the path inside the user job container (e.g. "/sandbox/tmpfs"), rooted at
        // the container sandbox. Re-root it onto the host slot sandbox so the tmpfs is created
        // inside the slot.
        std::filesystem::path relativeMountPath;
        for (std::string_view sandboxRoot : {"/slot/sandbox", "/sandbox"}) {
            if (mountPath.Path().native().starts_with(sandboxRoot)) {
                relativeMountPath = mountPath.Path().lexically_relative(sandboxRoot);
                break;
            }
        }
        auto path = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, relativeMountPath.string()));

        YT_VERIFY(volume->Size);
        auto config = New<TMountTmpfsConfig>();
        config->Path = path;
        config->Size = *volume->Size;
        config->UserId = volume->UserId;

        YT_TLOG_DEBUG("Creating tmpfs volume")
            .With("Tag", tag)
            .With("JobId", jobId)
            .With("Config", ConvertToYsonString(config, EYsonFormat::Text));

        return BIND(
            [
                tagSet,
                volumeId = volume->VolumeId,
                sandboxPath,
                volumeCreateTimeGuard = std::move(volumeCreateTimeGuard),
                config = std::move(config),
                tmpfsIndex = volume->Index,
                this,
                this_ = MakeStrong(this)
            ] {
                try {
                    if (config->Path != sandboxPath) {
                        if (NFS::Exists(config->Path)) {
                            THROW_ERROR_EXCEPTION("Target path already exists")
                                .With("target", config->Path);
                        }
                        NFS::MakeDirRecursive(config->Path);
                    }

                    RunTool<TMountTmpfsAsRootTool>(config);

                    TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                        .Update(VolumeCounters().Increment(tagSet));
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created")
                        .Increment(1);

                    auto result = New<TTmpfsVolumeResult>(
                        std::move(volumeId),
                        EVolumeType::Tmpfs,
                        New<THostTmpfsVolume>(
                            tagSet,
                            config->Path,
                            Invoker_,
                            DetachUnmount_),
                        tmpfsIndex);
                    return StaticPointerCast<TVolumeResult>(std::move(result));
                } catch (const std::exception& ex) {
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);
                    throw;
                }
            })
            .AsyncVia(Invoker_)
            .Run()
            .ToUncancelable();
    }

    void FindTmpfsMountPathsInLocation(const std::string& locationPath, std::vector<std::string>& mountPaths) const
    {
        auto mountPoints = NFS::GetMountPoints("/proc/mounts");
        for (const auto& mountPoint : mountPoints) {
            if (mountPoint.Path.starts_with(locationPath + "/")) {
                mountPaths.push_back(mountPoint.Path);
            }
        }
    }

    TFuture<void> CleanupTmpfsMountPaths(std::vector<std::string>&& mountPaths) const
    {
        return BIND([mountPaths = std::move(mountPaths), detachUnmount = DetachUnmount_] {
            for (const auto& path : mountPaths) {
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
                config->Detach = detachUnmount;
                RunTool<TUmountAsRootTool>(config);
            }
        })
        .AsyncVia(Invoker_)
        .Run();
    }
};

DEFINE_REFCOUNTED_TYPE(THostVolumeManager)

////////////////////////////////////////////////////////////////////////////////

TFuture<IHostVolumeManagerPtr> CreateHostVolumeManager(
    const std::vector<TSlotLocationConfigPtr>& locations,
    IInvokerPtr invoker,
    bool detachedTmpfsUmount)
{
    auto volumeManager = New<THostVolumeManager>(
        std::move(invoker),
        detachedTmpfsUmount);

    return volumeManager->Initialize(locations).Apply(BIND([=] {
        return static_cast<IHostVolumeManagerPtr>(volumeManager);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
