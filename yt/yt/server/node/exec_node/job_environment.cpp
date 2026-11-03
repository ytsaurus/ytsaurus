#include "job_environment.h"

#include "bootstrap.h"
#include "job_environment_health_checker.h"

#include "environment/cri/job_environment.h"

#include "environment/porto/health_checker.h"
#include "environment/porto/job_environment.h"

#include "environment/simple/job_environment.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/job_proxy/config.h>

namespace NYT::NExecNode {

using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentHealthCheckerPtr CreateJobEnvironmentHealthChecker(TNonNullPtr<IBootstrap> bootstrap)
{
    if (bootstrap->GetConfig()->ExecNode->SlotManager->JobEnvironment.GetType() == EJobEnvironmentType::Porto) {
        return CreatePortoJobEnvironmentHealthChecker(bootstrap);
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(NJobProxy::TJobEnvironmentConfig config, TNonNullPtr<IBootstrap> bootstrap)
{
    switch (config.GetType()) {
        case EJobEnvironmentType::Simple: {
            return CreateSimpleJobEnvironment(
                std::move(config),
                bootstrap);
        }

        case EJobEnvironmentType::Porto: {
#ifdef _linux_
            return CreatePortoJobEnvironment(
                std::move(config),
                bootstrap);
#else
            THROW_ERROR_EXCEPTION("Porto is not supported for this platform");
#endif
        }

        case EJobEnvironmentType::Testing: {
            return CreateTestingJobEnvironment(
                std::move(config),
                bootstrap);
        }

        case EJobEnvironmentType::Cri: {
            return CreateCriJobEnvironment(
                std::move(config),
                bootstrap);
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
