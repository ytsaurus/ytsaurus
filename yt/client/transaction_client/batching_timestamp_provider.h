#pragma once

#include "public.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration updatePeriod,
    TDuration batchPeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
