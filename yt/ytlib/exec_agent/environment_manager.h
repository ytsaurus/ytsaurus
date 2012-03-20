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
    TEnvironmentManager(TEnvironmentMapPtr environmentMap);

    IProxyControllerPtr CreateProxyController(
        const Stroka& envName,
        const TJobId& jobId,
        const Stroka& workingDirectory);

    void Register(
        const Stroka& envType, 
        IEnvironmentBuilderPtr envBuilder);

private:
    TEnvironmentMapPtr EnvironmentMap;
    yhash_map<Stroka, IEnvironmentBuilderPtr> Builders;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
