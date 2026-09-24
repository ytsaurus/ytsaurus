#pragma once

#include "public.h"

#include <vector>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TChaosLease
    : public TRefCounted
{
    TDuration Timeout;
    //! Per-connection cache leaves this field zero; direct GetChaosLease fills it.
    TInstant LastPingTime;
    std::vector<NObjectClient::TCellId> CoordinatorCellIds;
};

DEFINE_REFCOUNTED_TYPE(TChaosLease)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
