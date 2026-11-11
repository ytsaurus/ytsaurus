#include "job_workspace_builder.h"

#include <yt/yt/server/node/exec_node/environment/common/host_slot.h>

#include <yt/yt/server/node/exec_node/allocation.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/slot.h>
#include <yt/yt/server/node/exec_node/volume.h>

#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;
using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static const std::string SetupCommandsTag = "setup";

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobWorkspaceBuilder
    : public TJobWorkspaceBuilder
{
public:
    TSimpleJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(context),
            std::move(directoryManager))
    { }

private:
    THostUserSlotPtr GetSlot() const
    {
        return StaticPointerCast<THostUserSlot>(Context_.Slot);
    }

    TRootFS MakeWritableRootFS()
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.RootVolume);

        auto binds = Context_.Binds;

        for (const auto& bind : ResultHolder_.RootBinds) {
            binds.push_back(bind);
        }

        return TRootFS{
            .RootPath = ResultHolder_.RootVolume->GetPath(),
            .IsRootReadOnly = false,
            .Binds = std::move(binds),
        };
    }

    TFuture<void> DoPrepareLayers() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Layer preparation is not needed");

        ValidateJobPhase(EJobPhase::CachingArtifacts);
        SetJobPhase(EJobPhase::PreparingLayers);

        return OKFuture;
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Root volume preparation is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::PreparingSlotDirectories);
        SetJobPhase(EJobPhase::PreparingVolumes);

        if (Context_.FSSecretary->GetRootVolumeParams()) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::LayerUnpackingFailed,
                "Root volume are not supported in simple job environment"));
        }

        if (Context_.FSSecretary->GetDockerImage()) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::DockerImagePullingFailed,
                "External docker image is not supported in simple job environment"));
        }

        return OKFuture;
    }

    TFuture<void> DoPrepareNonRootVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        SetNowTime(TimePoints_.PrepareNonRootVolumesStartTime);

        const auto& volumes = Context_.FSSecretary->GetNonRootVolumesToPrepare();
        if (volumes.empty()) {
            SetNowTime(TimePoints_.PrepareNonRootVolumesFinishTime);
            return OKFuture;
        }

        auto slot = GetSlot();

        return slot->PrepareNonRootVolumes(
            Context_.Job->GetId(),
            ResultHolder_.RootVolume,
            volumes,
            Context_.UserSandboxOptions.JobVolumeMounts)
            .AsUnique().Apply(BIND([
                jobId = Context_.Job->GetId(),
                slot,
                this,
                this_ = MakeStrong(this)
            ] (TErrorOr<std::vector<TVolumeResultPtr>>&& volumeResultsOrError) {
                if (!volumeResultsOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::NonRootVolumePreparationFailed, "Failed to prepare non-root volumes")
                        .With(volumeResultsOrError);
                }

                auto& volumeResults = volumeResultsOrError.Value();

                YT_TLOG_DEBUG("Prepared non-root volumes")
                    .With("Volumes", MakeFormattableView(
                        volumeResults,
                        [] (TNonNullPtr<TStringBuilderBase> builder, const TVolumeResultPtr& result) {
                            builder->AppendFormat("{VolumeId: %v, VolumePath: %v}",
                                result->VolumeId,
                                result->Volume->GetPath());
                        }));

                Context_.PreparedNonRootVolumes = std::move(volumeResults);

                SetNowTime(TimePoints_.PrepareNonRootVolumesFinishTime);
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoBindRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingVolumes);
        SetJobPhase(EJobPhase::LinkingVolumes);

        YT_TLOG_DEBUG("Root volume binding is not needed in simple workspace");

        return OKFuture;
    }

    TFuture<void> DoLinkVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ResultHolder_.PreparedNonRootVolumes = std::move(Context_.PreparedNonRootVolumes);

        YT_TLOG_DEBUG("Link non-root volumes is not needed in simple workspace");

        return OKFuture;
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::ValidatingRootFS);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_TLOG_INFO("Started preparing sandbox directories");

        return Context_.Slot->PrepareSandboxDirectories(Context_.UserSandboxOptions)
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                MakeArtifactSymlinks();

                YT_TLOG_INFO("Finished preparing sandbox directories");
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoRunSetupCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Setup command is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        return OKFuture;
    }

    TFuture<void> DoRunCustomPreparations() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("There are no custom preparations in simple workspace");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningCustomPreparations);

        return OKFuture;
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("GPU check is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::RunningCustomPreparations);

        return OKFuture;
    }
};

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateSimpleJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager)
{
    return New<TSimpleJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(context),
        std::move(directoryManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
