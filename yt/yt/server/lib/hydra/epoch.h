#pragma once

#include "public.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

NElection::TEpochId GetCurrentEpochId();

////////////////////////////////////////////////////////////////////////////////

class TCurrentEpochIdGuard
{
public:
    TCurrentEpochIdGuard(const TCurrentEpochIdGuard&) = delete;
    TCurrentEpochIdGuard(TCurrentEpochIdGuard&&) = delete;

    explicit TCurrentEpochIdGuard(NElection::TEpochId epochId);
    ~TCurrentEpochIdGuard();
};

////////////////////////////////////////////////////////////////////////////////

IInvokerPtr CreateEpochIdInjectingInvoker(
    IInvokerPtr underlying,
    NElection::TEpochId epochId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
