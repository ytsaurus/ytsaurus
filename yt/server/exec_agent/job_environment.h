#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/cgroup/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/node.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJobEnvironment
    : public virtual TRefCounted
{
    virtual TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        const TJobId& jobId,
        const TOperationId& operationId) = 0;

    virtual void CleanProcesses(int slotIndex) = 0;

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path) = 0;

    //! User id for user job processes.
    virtual int GetUserId(int slotIndex) const = 0;

    virtual bool IsEnabled() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(
    NYTree::INodePtr config,
    const NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
