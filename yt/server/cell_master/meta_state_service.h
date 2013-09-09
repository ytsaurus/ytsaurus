#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

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

    void ValidateActiveLeader();

private:
    virtual TClosure PrepareHandler(TClosure handler) override;

    void OnStopEpoch();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
