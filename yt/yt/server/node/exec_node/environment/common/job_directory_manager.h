#pragma once

#include <yt/yt/server/node/exec_node/job_directory_manager.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

IJobDirectoryManagerPtr CreateHostJobDirectoryManager(
    IInvokerPtr invoker,
    const std::string& path,
    bool detachedTmpfsUmount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
