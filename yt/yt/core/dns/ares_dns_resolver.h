#pragma once

#include "public.h"

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IDnsResolver> CreateAresDnsResolver(
    int retries,
    TDuration resolveTimeout,
    TDuration maxResolveTimeout,
    TDuration warningTimeout,
    std::optional<double> jitter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

