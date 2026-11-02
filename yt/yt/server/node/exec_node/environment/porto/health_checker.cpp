#include "health_checker.h"

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_environment_health_checker.h>
#include <yt/yt/server/node/exec_node/private.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/porto_health_checker.h>

#include <yt/yt/core/actions/bind.h>

namespace NYT::NExecNode {

using namespace NContainers;

////////////////////////////////////////////////////////////////////////////////

class TPortoJobEnvironmentHealthChecker
    : public IJobEnvironmentHealthChecker
{
public:
    explicit TPortoJobEnvironmentHealthChecker(TNonNullPtr<IBootstrap> bootstrap)
        : HealthChecker_(New<TPortoHealthChecker>(
            New<TPortoExecutorDynamicConfig>(),
            bootstrap->GetControlInvoker(),
            ExecNodeLogger()))
    { }

    void Start(TClosure onSuccess, TCallback<void(const TError&)> onFailed) override
    {
        HealthChecker_->SubscribeSuccess(std::move(onSuccess));
        HealthChecker_->SubscribeFailed(BIND([onFailed = std::move(onFailed)] (const TError& error) {
            onFailed(error.Wrap(TError(EErrorCode::PortoHealthCheckFailed, "Porto health check failed")));
        }));
        HealthChecker_->Start();
    }

    void OnDynamicConfigChanged(const TSlotManagerDynamicConfigPtr& newConfig) override
    {
        if (auto portoConfig = newConfig->JobEnvironment.TryGetConcrete<NJobProxy::EJobEnvironmentType::Porto>()) {
            HealthChecker_->OnDynamicConfigChanged(portoConfig->PortoExecutor);
        }
    }

private:
    const TPortoHealthCheckerPtr HealthChecker_;
};

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentHealthCheckerPtr CreatePortoJobEnvironmentHealthChecker(TNonNullPtr<IBootstrap> bootstrap)
{
    return New<TPortoJobEnvironmentHealthChecker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
