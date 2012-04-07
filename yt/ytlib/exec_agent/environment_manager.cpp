#include "stdafx.h"
#include "environment_manager.h"
#include "environment.h"
#include "config.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TEnvironmentManager::TEnvironmentManager(TEnvironmentManagerConfigPtr config)
    : Config(config)
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
    auto env = Config->FindEnvironment(envName);

    auto it = Builders.find(env->Type);
    if (it == Builders.end()) {
        ythrow yexception() << Sprintf("No such environment type %s", ~env->Type.Quote());
    }

    return it->second->CreateProxyController(
        env->GetOptions(),
        jobId,
        workingDirectory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
