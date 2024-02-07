#pragma once

#include "public.h"
#include "job_workspace_builder.h"

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IJobEnvironment
    : public virtual TRefCounted
{
    virtual void Init(int slotCount, double cpuLimit, double idleCpuFraction) = 0;

    virtual TFuture<void> RunJobProxy(
        const NJobProxy::TJobProxyInternalConfigPtr& config,
        ESlotType slotType,
        int slotIndex,
        const TString& workingDirectory,
        TJobId jobId,
        TOperationId operationId,
        const std::optional<TNumaNodeInfo>& numaNodeAffinity) = 0;

    virtual void CleanProcesses(int slotIndex, ESlotType slotType = ESlotType::Common) = 0;

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int locationIndex) = 0;

    //! User id for user job processes.
    virtual int GetUserId(int slotIndex) const = 0;

    virtual bool IsEnabled() const = 0;

    virtual void UpdateCpuLimit(double cpuLimit) = 0;

    virtual double GetCpuLimit(ESlotType slotType) const = 0;

    virtual i64 GetMajorPageFaultCount() const = 0;

    virtual TFuture<void> RunSetupCommands(
        int slotIndex,
        ESlotType slotType,
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const TString& user,
        const std::optional<std::vector<NContainers::TDevice>>& devices,
        int startIndex) = 0;

    virtual TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) = 0;

    virtual void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& oldConfig,
        const TSlotManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(
    NYTree::INodePtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
