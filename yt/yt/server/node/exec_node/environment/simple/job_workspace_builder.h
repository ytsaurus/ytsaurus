#pragma once

#include <yt/yt/server/node/exec_node/job_workspace_builder.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateSimpleJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
