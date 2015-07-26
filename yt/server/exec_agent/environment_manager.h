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
        const TSlot& slot,
        const Stroka& workingDirectory);

    void Register(
        const Stroka& envType,
        IEnvironmentBuilderPtr envBuilder);

private:
    TEnvironmentManagerConfigPtr Config;
    yhash_map<Stroka, IEnvironmentBuilderPtr> Builders;

};

DEFINE_REFCOUNTED_TYPE(TEnvironmentManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
