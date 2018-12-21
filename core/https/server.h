#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/http/public.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

NHttp::IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
