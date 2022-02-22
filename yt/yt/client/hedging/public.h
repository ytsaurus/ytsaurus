#pragma once

#include <yt/yt/client/api/public.h>

namespace NYT::NHedgingClient::NRpc {

DECLARE_REFCOUNTED_STRUCT(TCounter);
DECLARE_REFCOUNTED_STRUCT(TLagPenaltyProviderCounters);
DECLARE_REFCOUNTED_CLASS(IClientsCache);

} // namespace NYT::NHedgingClient::NRpc
