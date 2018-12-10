#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/http/public.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

NHttp::IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
