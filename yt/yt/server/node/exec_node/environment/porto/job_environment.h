#pragma once

#include <yt/yt/server/node/exec_node/job_environment.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

IJobEnvironmentPtr CreatePortoJobEnvironment(
    NJobProxy::TJobEnvironmentConfig config,
    TNonNullPtr<IBootstrap> bootstrap);

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
