#pragma once

#include <yt/yt/server/node/exec_node/job_environment.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateSimpleJobEnvironment(
    NJobProxy::TJobEnvironmentConfig config,
    TNonNullPtr<IBootstrap> bootstrap);

IJobEnvironmentPtr CreateTestingJobEnvironment(
    NJobProxy::TJobEnvironmentConfig config,
    TNonNullPtr<IBootstrap> bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
