#pragma once

#include "public.h"

#include <server/hydra/hydra_service.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceBase
    : public NHydra::THydraServiceBase
{
protected:
    TBootstrap* Bootstrap;

    THydraServiceBase(
        TBootstrap* bootstrap,
        const Stroka& serviceName,
        const NLog::TLogger& logger);

private:
    virtual void BeforeInvoke() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
