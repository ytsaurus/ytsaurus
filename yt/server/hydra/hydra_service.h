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
        const Stroka& loggingCategory);

    IInvokerPtr AutomatonInvoker;
    IInvokerPtr EpochAutomatonInvoker;

    void ValidateActiveLeader();

private:
    // Avoid name clash when inheriting from both THydraServiceBase and TCompositeAutomatonPart.
    IHydraManagerPtr ServiceHydraManager;

    void OnLeaderActive();
    void OnStopLeading();
    void OnStopFollowing();

    void OnStopEpoch();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
