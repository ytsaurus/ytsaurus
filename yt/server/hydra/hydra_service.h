#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceBase
    : public NRpc::TServiceBase
{
protected:
    THydraServiceBase(
        IHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker,
        const NRpc::TServiceId& serviceId,
        const NLog::TLogger& logger,
        int protocolVersion = 0);

    IInvokerPtr AutomatonInvoker_;
    IInvokerPtr EpochAutomatonInvoker_;

    void ValidateActiveLeader();

private:
    // Avoid name clash when inheriting from both THydraServiceBase and TCompositeAutomatonPart.
    IHydraManagerPtr ServiceHydraManager_;

    void OnLeaderActive();
    void OnStopLeading();

    virtual bool IsUp() const override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
