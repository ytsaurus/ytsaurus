#include "stdafx.h"
#include "environment_manager.h"
#include "environment.h"
#include "common.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TEnvironmentMap::TEnvironmentMap()
{
    Register("environments", Environments);
}

TEnvironmentPtr TEnvironmentMap::FindEnvironment(const Stroka& name)
{
    auto it = Environments.find(name);
    if (it == Environments.end()) {
        LOG_WARNING_AND_THROW(yexception(), "No such environment %s", ~name);
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

TEnvironmentManager::TEnvironmentManager(TEnvironmentMapPtr environmentMap)
    : EnvironmentMap(environmentMap)
{ }

void TEnvironmentManager::Register(
    const Stroka& envType, 
    IEnvironmentBuilderPtr envBuilder)
{
    YVERIFY(Builders.insert(MakePair(envType, envBuilder)).second);
}

IProxyControllerPtr TEnvironmentManager::CreateProxyController(
    const Stroka& envName, 
    const TJobId& jobId, 
    const Stroka& workingDirectory)
{
    auto env = EnvironmentMap->FindEnvironment(envName);

    auto it = Builders.find(env->Type);
    if (it == Builders.end()) {
        LOG_WARNING_AND_THROW(yexception(), "No such environment type %s", ~env->Type);
    }

    return it->second->CreateProxyController(
        env->Config,
        jobId,
        workingDirectory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
