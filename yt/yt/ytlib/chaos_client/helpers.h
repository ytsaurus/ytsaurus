#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

TError CreateChaosLeaseNotKnownError(TChaosLeaseId chaosLeaseId);

[[noreturn]] void ThrowChaosLeaseNotKnown(TChaosLeaseId chaosLeaseId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
