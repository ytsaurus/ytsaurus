#pragma once

#include "private.h"
#include "chunk_cache.h"
#include "job.h"

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/chunk.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/containers/cri/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

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
    TString CommandUser;

    TArtifactDownloadOptions ArtifactDownloadOptions;

    std::vector<TArtifact> Artifacts;
    std::vector<NContainers::TBind> Binds;
    std::vector<NDataNode::TArtifactKey> RootVolumeLayerArtifactKeys;
    std::vector<NDataNode::TArtifactKey> GpuCheckVolumeLayerArtifactKeys;
    std::vector<TShellCommandConfigPtr> SetupCommands;
    std::optional<TString> DockerImage;
    NContainers::NCri::TCriAuthConfigPtr DockerAuth;

    bool NeedGpuCheck;
    std::optional<TString> GpuCheckBinaryPath;
    std::optional<std::vector<TString>> GpuCheckBinaryArgs;
    EGpuCheckType GpuCheckType;
    std::vector<NContainers::TDevice> GpuDevices;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuildingResult
{
    IVolumePtr RootVolume;
    IVolumePtr GpuCheckVolume;
    std::optional<TString> DockerImage;
    std::vector<TString> TmpfsPaths;
    std::vector<NContainers::TBind> RootBinds;
    int SetupCommandCount = 0;

    TError LastBuildError;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuilderTimePoints
{
    std::optional<TInstant> PrepareRootVolumeStartTime;
    std::optional<TInstant> PrepareRootVolumeFinishTime;

    std::optional<TInstant> PrepareGpuCheckVolumeStartTime;
    std::optional<TInstant> PrepareGpuCheckVolumeFinishTime;

    std::optional<TInstant> GpuCheckStartTime;
    std::optional<TInstant> GpuCheckFinishTime;
};

////////////////////////////////////////////////////////////////////////////////

class TJobWorkspaceBuilder
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(EJobPhase phase), UpdateBuilderPhase);
    DEFINE_SIGNAL(void(i64 compressedDataSize, bool cacheHit), UpdateArtifactStatistics);
    DEFINE_SIGNAL(void(TJobWorkspaceBuilderTimePoints), UpdateTimePoints);

public:
    TJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager);

    TFuture<TJobWorkspaceBuildingResult> Run();

protected:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const IInvokerPtr Invoker_;
    TJobWorkspaceBuildingContext Context_;
    const IJobDirectoryManagerPtr DirectoryManager_;

    TJobWorkspaceBuildingResult ResultHolder_;

    TJobWorkspaceBuilderTimePoints TimePoints_;

    const NLogging::TLogger& Logger;

    virtual TFuture<void> DoPrepareRootVolume() = 0;

    virtual TFuture<void> DoPrepareGpuCheckVolume() = 0;

    virtual TFuture<void> DoPrepareSandboxDirectories() = 0;

    virtual TFuture<void> DoRunSetupCommand() = 0;

    virtual TFuture<void> DoRunGpuCheckCommand() = 0;

    void ValidateJobPhase(EJobPhase expectedPhase) const;

    void SetJobPhase(EJobPhase phase);

    void UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit);

    void MakeArtifactSymlinks();

    void PrepareArtifactBinds();

    void SetNowTime(std::optional<TInstant>& timeField);

private:
    template<TFuture<void>(TJobWorkspaceBuilder::*Step)()>
    TCallback<TFuture<void>()> MakeStep();

    template<TFuture<void>(TJobWorkspaceBuilder::*Step)()>
    TFuture<void> GuardedAction();

    template<TFuture<void>(TJobWorkspaceBuilder::*Step)()>
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
    IJobDirectoryManagerPtr directoryManager);

#endif

TJobWorkspaceBuilderPtr CreateCriJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    NContainers::NCri::ICriImageCachePtr imageCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
