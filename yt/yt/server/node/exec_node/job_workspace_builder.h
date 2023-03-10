#pragma once

#include "public.h"
#include "private.h"
#include "chunk_cache.h"
#include "job.h"
#include "slot.h"

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/chunk.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

namespace NYT::NExecNode
{

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuildSettings
{
    TUserSandboxOptions UserSandboxOptions;
    ISlotPtr Slot;
    TJobPtr Job;
    TString CommandUser;

    TArtifactDownloadOptions ArtifactDownloadOptions;

    std::vector<TArtifact> Artifacts;
    std::vector<NContainers::TBind> Binds;
    std::vector<NDataNode::TArtifactKey> LayerArtifactKeys;
    std::vector<NJobAgent::TShellCommandConfigPtr> SetupCommands;

    bool NeedGpuCheck;
    bool TestExtraGpuCheckCommandFailure;
    std::optional<TString> GpuCheckBinaryPath;
    std::optional<std::vector<TString>> GpuCheckBinaryArgs;
    EGpuCheckType GpuCheckType;
    std::vector<NContainers::TDevice> GpuDevices;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobWorkspaceBuildResult
{
    IVolumePtr RootVolume;
    std::vector<TString> TmpfsPaths;
    std::vector<NContainers::TBind> RootBinds;
    int SetupCommandCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TJobWorkspaceBuilder
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(EJobPhase phase), UpdateBuilderPhase);
    DEFINE_SIGNAL(void(i64 compressedDataSize, bool cacheHit), UpdateArtifactStatistics);
    DEFINE_SIGNAL(void(TJobWorkspaceBuilderPtr), UpdateTimers);

    DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, VolumePrepareStartTime);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, VolumePrepareFinishTime);

    DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, GpuCheckStartTime);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, GpuCheckFinishTime);

public:
    TJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildSettings settings,
        IJobDirectoryManagerPtr directoryManager);

    TFuture<TJobWorkspaceBuildResult> Run();

protected:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const IInvokerPtr Invoker_;
    const TJobWorkspaceBuildSettings Settings_;
    const IJobDirectoryManagerPtr DirectoryManager_;

    TJobWorkspaceBuildResult ResultHolder_;

    virtual TFuture<void> DoPrepareSandboxDirectories() = 0;

    virtual TFuture<void> DoPrepareRootVolume() = 0;

    virtual TFuture<void> DoRunSetupCommand() = 0;

    virtual TFuture<void> DoRunGpuCheckCommand() = 0;

    void ValidateJobPhase(EJobPhase expectedPhase) const;

    void SetJobPhase(EJobPhase phase);

    void UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit);

    NContainers::TRootFS MakeWritableRootFS();

private:
    TFuture<void> GuardedAction(const std::function<TFuture<void>()>& action);

    TFuture<void> PrepareSandboxDirectories();

    TFuture<void> PrepareRootVolume();

    TFuture<void> RunSetupCommand();

    TFuture<void> RunGpuCheckCommand();

};

DEFINE_REFCOUNTED_TYPE(TJobWorkspaceBuilder)

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateSimpleJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildSettings settings,
    IJobDirectoryManagerPtr directoryManager);

#ifdef _linux_

TJobWorkspaceBuilderPtr CreatePortoJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildSettings settings,
    IJobDirectoryManagerPtr directoryManager);

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
