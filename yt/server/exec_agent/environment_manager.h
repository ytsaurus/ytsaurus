#pragma once

#include "public.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TEnvironmentManager
    : public TRefCounted
{
public:
    TEnvironmentManager(TEnvironmentManagerConfigPtr config);

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
