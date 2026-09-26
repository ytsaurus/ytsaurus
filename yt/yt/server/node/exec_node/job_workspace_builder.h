#pragma once

#include "artifact.h"
#include "artifact_cache.h"
#include "job.h"
#include "job_gpu_checker.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/node/data_node/chunk.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/containers/cri/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuildingContext
{
    NLogging::TLogger Logger;

    TUserSandboxOptions UserSandboxOptions;
    IUserSlotPtr Slot;
    TJobPtr Job;
    std::string CommandUser;

    TArtifactDownloadOptions ArtifactDownloadOptions;

    TJobFSSecretaryPtr FSSecretary;
    std::vector<NContainers::TBind> Binds;
    std::vector<TShellCommandConfigPtr> SetupCommands;
    NContainers::NCri::TCriAuthConfigPtr DockerAuth;
    std::vector<TVolumeResultPtr> PreparedNonRootVolumes;
    std::vector<TVolumeResultPtr> ReusedNonRootVolumes;

    bool NeedGpu = false;
    std::optional<TGpuCheckOptions> GpuCheckOptions;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuildingResult
{
    IVolumePtr RootVolume;
    IVolumePtr GpuCheckVolume;
    std::optional<std::string> DockerImage;
    std::optional<std::string> DockerImageId;
    std::vector<TVolumeResultPtr> PreparedNonRootVolumes;
    std::vector<NContainers::TBind> RootBinds;
    int SetupCommandCount = 0;

    TError LastBuildError;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuilderTimePoints
{
    std::optional<TInstant> PrepareLayersStartTime;
    std::optional<TInstant> PrepareLayersFinishTime;

    std::optional<TInstant> PrepareRootVolumeStartTime;
    std::optional<TInstant> PrepareRootVolumeFinishTime;

    std::optional<TInstant> ValidateRootFSStartTime;
    std::optional<TInstant> ValidateRootFSFinishTime;

    std::optional<TInstant> PrepareNonRootVolumesStartTime;
    std::optional<TInstant> PrepareNonRootVolumesFinishTime;

    std::optional<TInstant> PrepareGpuCheckVolumeStartTime;
    std::optional<TInstant> PrepareGpuCheckVolumeFinishTime;

    std::optional<TInstant> GpuCheckStartTime;
    std::optional<TInstant> GpuCheckFinishTime;

    std::optional<TInstant> LinkVolumesStartTime;
    std::optional<TInstant> LinkVolumesFinishTime;
};

////////////////////////////////////////////////////////////////////////////////

class TJobWorkspaceBuilder;

// A step is a pointer to a workspace building method of any builder class.
template <auto Step>
concept CWorkspaceBuilderStep = requires
{
    static_cast<TFuture<void>(TJobWorkspaceBuilder::*)()>(Step);
};

////////////////////////////////////////////////////////////////////////////////

class TJobWorkspaceBuilder
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(EJobPhase phase), UpdateBuilderPhase);
    DEFINE_SIGNAL(void(i64 compressedDataSize, bool cacheHit, bool isLayer), UpdateArtifactStatistics);
    DEFINE_SIGNAL(void(TJobWorkspaceBuilderTimePoints), UpdateTimePoints);

public:
    TJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager);

    virtual TFuture<void> Run();

    TJobWorkspaceBuildingResult ExtractResult();

protected:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const IInvokerPtr Invoker_;
    TJobWorkspaceBuildingContext Context_;
    const IJobDirectoryManagerPtr DirectoryManager_;

    bool ResultExtracted_ = false;
    TJobWorkspaceBuildingResult ResultHolder_;

    TJobWorkspaceBuilderTimePoints TimePoints_;

    const NLogging::TLogger& Logger;

    TFuture<void> DoBuildSlotRootDirectory();

    virtual TFuture<void> DoPrepareLayers() = 0;

    virtual TFuture<void> DoPrepareRootVolume() = 0;

    virtual TFuture<void> DoPrepareNonRootVolumes() = 0;

    // Only the porto workspace builder runs a preliminary GPU check, so only it prepares
    // the check volume. Other builders never run this step.
    virtual TFuture<void> DoPrepareGpuCheckVolume();

    virtual TFuture<void> DoBindRootVolume() = 0;

    virtual TFuture<void> DoLinkVolumes() = 0;

    virtual TFuture<void> DoValidateRootFS() = 0;

    virtual TFuture<void> DoPrepareSandboxDirectories() = 0;

    virtual TFuture<void> DoRunSetupCommand() = 0;

    virtual TFuture<void> DoRunCustomPreparations() = 0;

    // Only the porto workspace builder applies network priority.
    virtual TFuture<void> DoApplyNetworkPriority();

    virtual TFuture<void> DoRunGpuCheckCommand() = 0;

    void ValidateJobPhase(EJobPhase expectedPhase) const;

    void SetJobPhase(EJobPhase phase);

    void UpdateArtifactStatistics(
        i64 compressedDataSize,
        bool cacheHit,
        bool isLayer);

    void MakeArtifactSymlinks();

    //! We first create files for artifact binds and then create actual container
    //! binds for artifacts. If we do not create files ourselves porto will
    //! create them with root ownership which is not what we need.
    void MakeFilesForArtifactBinds();

    void SetNowTime(std::optional<TInstant>& timeField);

    template <auto Step> requires CWorkspaceBuilderStep<Step>
    TCallback<TFuture<void>()> MakeStep();

    TFuture<void> FinishRun(TFuture<void> future);

private:
    template <auto Step> requires CWorkspaceBuilderStep<Step>
    TFuture<void> GuardedAction();

    template <auto Step> requires CWorkspaceBuilderStep<Step>
    constexpr const char* GetStepName();
};

DEFINE_REFCOUNTED_TYPE(TJobWorkspaceBuilder)

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateSimpleJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager);

#ifdef _linux_

TJobWorkspaceBuilderPtr CreatePortoJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    TGpuManagerPtr gpuManager);

#endif

TJobWorkspaceBuilderPtr CreateCriJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    NContainers::NCri::ICriImageCachePtr imageCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
