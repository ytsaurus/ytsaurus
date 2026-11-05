#pragma once

#include <yt/yt/server/node/exec_node/public.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentHealthCheckerPtr CreatePortoJobEnvironmentHealthChecker(TNonNullPtr<IBootstrap> bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
