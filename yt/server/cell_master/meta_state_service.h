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

private:
    virtual void InvokerHandler(
        NRpc::IServiceContextPtr context,
        IInvokerPtr invoker,
        TClosure handler) override;

    void OnStopEpoch();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
