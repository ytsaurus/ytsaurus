#pragma once

#include "token_authenticator.h"
#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateYCIAMTokenAuthenticator(
    TYCIAMTokenAuthenticatorConfigPtr config,
    NConcurrency::IPollerPtr poller,
    ICypressUserManagerPtr userManager,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
