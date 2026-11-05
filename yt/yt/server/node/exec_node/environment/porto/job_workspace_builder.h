#pragma once

#include <yt/yt/server/node/exec_node/job_workspace_builder.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

TJobWorkspaceBuilderPtr CreatePortoJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    TGpuManagerPtr gpuManager);

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
