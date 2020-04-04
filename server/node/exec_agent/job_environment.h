#pragma once

#include "public.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/server/node/data_node/artifact.h>
#include <yt/server/node/data_node/public.h>

#include <yt/ytlib/cgroup/public.h>

#include <yt/core/actions/signal.h>

#include <yt/server/lib/containers/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/node.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJobEnvironment
    : public virtual TRefCounted
{
    virtual void Init(int slotCount, double jobsCpuLimit) = 0;

    virtual TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        TJobId jobId,
        TOperationId operationId) = 0;

    virtual void CleanProcesses(int slotIndex) = 0;

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int locationIndex) = 0;

    //! User id for user job processes.
    virtual int GetUserId(int slotIndex) const = 0;

    virtual bool IsEnabled() const = 0;

    virtual std::optional<i64> GetMemoryLimit() const = 0;

    virtual std::optional<double> GetCpuLimit() const = 0;

    virtual TFuture<void> RunSetupCommands(
        int slotIndex,
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const TString& user) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(
    NYTree::INodePtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
