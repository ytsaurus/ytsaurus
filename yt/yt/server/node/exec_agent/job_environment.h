#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJobEnvironment
    : public virtual TRefCounted
{
    virtual void Init(int slotCount, double cpuLimit) = 0;

    virtual TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        TJobId jobId,
        TOperationId operationId,
        const std::optional<TString>& stderrPath) = 0;

    virtual void CleanProcesses(int slotIndex) = 0;

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int locationIndex) = 0;

    //! User id for user job processes.
    virtual int GetUserId(int slotIndex) const = 0;

    virtual bool IsEnabled() const = 0;

    virtual void UpdateCpuLimit(double cpuLimit) = 0;

    virtual TFuture<void> RunSetupCommands(
        int slotIndex,
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const TString& user,
        const std::optional<std::vector<NContainers::TDevice>>& devices,
        int startIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(
    NYTree::INodePtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
