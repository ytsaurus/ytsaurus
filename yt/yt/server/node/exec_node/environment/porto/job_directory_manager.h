#pragma once

#include <yt/yt/server/node/exec_node/job_directory_manager.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

IJobDirectoryManagerPtr CreatePortoJobDirectoryManager(
    NExecNode::TJobDirectoryManagerDynamicConfigPtr config,
    bool enableDiskQuota,
    const std::string& path,
    int locationIndex);

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
