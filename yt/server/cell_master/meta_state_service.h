#pragma once

#include "public.h"

#include <ytlib/rpc/service.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    TBootstrap* Bootstrap;

    TMetaStateServiceBase(
        TBootstrap* bootstrap,
        const Stroka& serviceName,
        const Stroka& loggingCategory);


    void ValidateLeaderStatus();
    void ValidateInitialized();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
