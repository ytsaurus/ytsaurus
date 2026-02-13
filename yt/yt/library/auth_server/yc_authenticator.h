#pragma once

#include "cookie_authenticator.h"
#include "token_authenticator.h"
#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateYCIamTokenAuthenticator(
    TYCAuthenticatorConfigPtr config,
    NConcurrency::IPollerPtr poller,
    ICypressUserManagerPtr userManager,
    NProfiling::TProfiler profiler = {});

ICookieAuthenticatorPtr CreateYCSessionCookieAuthenticator(
    TYCAuthenticatorConfigPtr config,
    NConcurrency::IPollerPtr poller,
    ICypressUserManagerPtr userManager,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
