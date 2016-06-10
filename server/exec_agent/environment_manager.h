#pragma once

#include "public.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TEnvironmentManager
    : public TRefCounted
{
public:
    explicit TEnvironmentManager(TEnvironmentManagerConfigPtr config);

    IProxyControllerPtr CreateProxyController(
        const Stroka& environmentName,
        const TJobId& jobId,
        const TOperationId& operationId,
        TSlotPtr slot);

    void RegisterBuilder(
        const Stroka& environmentName,
        IEnvironmentBuilderPtr environmentBuilder);

private:
    const TEnvironmentManagerConfigPtr Config_;
    yhash_map<Stroka, IEnvironmentBuilderPtr> Builders_;

};

DEFINE_REFCOUNTED_TYPE(TEnvironmentManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
