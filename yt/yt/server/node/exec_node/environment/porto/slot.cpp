#include "slot.h"

#include "slot_location.h"
#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/slot_location.h>
#include <yt/yt/server/node/exec_node/volume_artifact.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

TPortoUserSlot::TPortoUserSlot(TUserSlotContext context)
    : TUserSlotBase(std::move(context))
{ }

void TPortoUserSlot::CleanJobWorkspaceResources(const TJobFSSecretary& fsSecretary)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    YT_VERIFY(VolumeManager_);

    THashSet<std::string> preservedVolumePaths;
    if (const auto& volume = fsSecretary.GetRootVolume()) {
        preservedVolumePaths.insert(volume->GetPath());
    }
    if (const auto& volume = fsSecretary.GetGpuCheckVolume()) {
        preservedVolumePaths.insert(volume->GetPath());
    }
    for (const auto& [_, result] : fsSecretary.GetNonRootVolumes()) {
        preservedVolumePaths.insert(result->Volume->GetPath());
    }

    CleanPortoSlotResources(
        Location_,
        SlotIndex_,
        GetVolumeManager(),
        *Bootstrap_->GetDynamicConfig()->ExecNode->SlotManager,
        preservedVolumePaths);
}

void TPortoUserSlot::CleanAllocationWorkspace()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    WaitFor(Location_->CleanAllocationDirectories(SlotIndex_))
        .ThrowOnError();
}

std::vector<TFuture<TOverlayData>> TPortoUserSlot::PrepareLayers(
    TJobId jobId,
    const std::vector<TOverlayLayerPreparationOptions>& layerOptions,
    const TArtifactDownloadContext& artifactDownloadContext)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    if (!VolumeManager_) {
        return {};
    }

    return GetVolumeManager()->PrepareOverlayLayers(jobId, layerOptions, artifactDownloadContext);
}

TFuture<IVolumePtr> TPortoUserSlot::PrepareRootVolume(
    std::vector<TOverlayData> overlayDataArray,
    const TBaseVolumeParamsPtr& volumeParams,
    const TVolumePreparationOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    if (!VolumeManager_) {
        return MakeFuture<IVolumePtr>(TError("Cannot prepare root volume without volume manager."));
    }

    return RunPreparationAction(
        /*actionName*/ "PrepareRootVolume",
        /*uncancelable*/ false,
        [&] {
            return GetVolumeManager()->PrepareVolume(
                std::move(overlayDataArray),
                volumeParams,
                MakeVolumePreparationContext(),
                options);
        });
}

TFuture<IVolumePtr> TPortoUserSlot::RbindRootVolume(
    const IVolumePtr& volume)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    if (!VolumeManager_) {
        return MakeFuture<IVolumePtr>(TError("Cannot bind root volume without volume manager."));
    }

    return RunPreparationAction(
        /*actionName*/ "RbindRootVolume",
        /*uncancelable*/ false,
        [&] {
            return GetVolumeManager()->RbindRootVolume(volume, GetSlotPath());
        });
}

TFuture<IVolumePtr> TPortoUserSlot::PrepareGpuCheckVolume(
    std::vector<TOverlayData> overlayDataArray,
    const TBaseVolumeParamsPtr& volumeParams,
    const TVolumePreparationOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    if (!VolumeManager_) {
        return MakeFuture<IVolumePtr>(TError("Porto layers and custom root FS are not supported"));
    }

    return RunPreparationAction(
        /*actionName*/ "PrepareGpuCheckVolume",
        /*uncancelable*/ false,
        [&] {
            return GetVolumeManager()->PrepareVolume(
                std::move(overlayDataArray),
                volumeParams,
                MakeVolumePreparationContext(),
                options);
        });
}

TFuture<std::vector<TVolumeResultPtr>> TPortoUserSlot::PrepareNonRootVolumes(
    TJobId jobId,
    const IVolumePtr& rootVolume,
    const std::vector<TBaseVolumeParamsPtr>& volumeParams,
    std::vector<std::vector<TOverlayData>> perVolumeOverlayData,
    const std::vector<TVolumeMountPtr>& volumeMounts)
{
    return DoPrepareNonRootVolumes(
        rootVolume,
        volumeParams,
        volumeMounts,
        BIND([&] (const std::string& sandboxPath) {
            return GetVolumeManager()->PrepareNonRootVolumes(
                sandboxPath,
                jobId,
                volumeParams,
                std::move(perVolumeOverlayData),
                volumeMounts,
                MakeVolumePreparationContext());
        }));
}

TFuture<void> TPortoUserSlot::LinkVolumes(
    const IVolumePtr& rootVolume,
    const std::vector<TVolumeResultPtr>& volumeResults,
    const std::vector<TVolumeMountPtr>& volumeMounts)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    if (!VolumeManager_) {
        static constexpr auto Message = "Failed to link volumes since volume manager is not initialized"_sb;
        YT_TLOG_WARNING(Message);
        return MakeFuture<void>(TError(Message));
    }

    if (!Bootstrap_->GetConfig()->ExecNode->SlotManager->EnableNonRootVolumes) {
        return OKFuture;
    }

    auto rootPath = GetRootPath(rootVolume);

    YT_TLOG_DEBUG("Linking volumes into root")
        .With("RootPath", rootPath)
        .With("Volumes", MakeFormattableView(
            volumeMounts,
            [] (TNonNullPtr<TStringBuilderBase> builder, const TVolumeMountPtr& volumeMount) {
                builder->AppendFormat(
                    "{VolumeId: %v, MountPath: %v}",
                    volumeMount->VolumeId,
                    volumeMount->MountPath);
            }));

    return RunPreparationAction(
        /*actionName*/ "LinkVolumes",
        /*uncancelable*/ true,
        [rootPath = std::move(rootPath), volumeResults, volumeMounts, this, this_ = MakeStrong(this)] {
            return GetVolumeManager()->LinkVolumes(rootPath, volumeResults, volumeMounts);
    });
}

TFuture<void> TPortoUserSlot::ValidateRootFS(const IVolumePtr& rootVolume) const
{
    VerifyEnabled();

    return Location_->ValidateRootFS(rootVolume);
}

TFuture<void> TPortoUserSlot::CreateRootVolumeDirectories(const IVolumePtr& rootVolume, int userId) const
{
    VerifyEnabled();

    return CreatePortoRootVolumeDirectories(Location_.Get(), rootVolume, userId);
}

TNonNullPtr<IPortoVolumeManager> TPortoUserSlot::GetVolumeManager() const
{
    return static_cast<IPortoVolumeManager*>(VolumeManager_.Get());
}

IUserSlotPtr CreatePortoUserSlot(TUserSlotContext context)
{
    auto invoker = context.Bootstrap->GetJobInvoker();
    return NewWithOffloadedDtor<TPortoUserSlot>(std::move(invoker), std::move(context));
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
