#pragma once

#include <yt/yt/server/node/exec_node/job_environment.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateCriJobEnvironment(
    NJobProxy::TJobEnvironmentConfig config,
    TNonNullPtr<IBootstrap> bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
