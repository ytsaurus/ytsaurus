#include "environment_manager.h"
#include "private.h"
#include "config.h"
#include "environment.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

TEnvironmentManager::TEnvironmentManager(TEnvironmentManagerConfigPtr config)
    : Config_(config)
{ }

void TEnvironmentManager::RegisterBuilder(
    const Stroka& environmentName,
    IEnvironmentBuilderPtr environmentBuilder)
{
    YCHECK(Builders_.insert(std::make_pair(environmentName, environmentBuilder)).second);
}

IProxyControllerPtr TEnvironmentManager::CreateProxyController(
    const Stroka& environmentName,
    const TJobId& jobId,
    const TOperationId& operationId,
    TSlotPtr slot)
{
    auto env = Config_->FindEnvironment(environmentName);

    auto it = Builders_.find(env->Type);
    if (it == Builders_.end()) {
        THROW_ERROR_EXCEPTION("No such environment type %Qv", env->Type);
    }

    return it->second->CreateProxyController(
        env->GetOptions(),
        jobId,
        operationId,
        slot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
