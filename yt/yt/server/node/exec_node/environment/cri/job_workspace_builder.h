#pragma once

#include <yt/yt/server/node/exec_node/job_workspace_builder.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateCriJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    NContainers::NCri::ICriImageCachePtr imageCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
