#pragma once

#include "public.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration updatePeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
