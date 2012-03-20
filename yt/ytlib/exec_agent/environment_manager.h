#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

class TEnvironmentManager
{
public:
    TEnvironmentManager(TEnvironmentManagerConfigPtr environmentMap);

    IProxyControllerPtr CreateProxyController(
        const Stroka& envName,
        const TJobId& jobId,
        const Stroka& workingDirectory);

    void Register(
        const Stroka& envType, 
        IEnvironmentBuilderPtr envBuilder);

private:
    TEnvironmentManagerConfigPtr Config;
    yhash_map<Stroka, IEnvironmentBuilderPtr> Builders;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
