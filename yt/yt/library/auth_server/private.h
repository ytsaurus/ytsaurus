#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, AuthLogger, "Auth");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, AuthProfiler, "/auth");

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf OAuthCookieRealm = "oauth:cookie";
constexpr TStringBuf OAuthTokenRealm = "oauth:token";

constexpr TStringBuf YCIAMTokenRealm = "yciam:token";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

