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
        const Stroka& loggingCategory);

private:
    virtual TClosure PrepareHandler(TClosure handler) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
