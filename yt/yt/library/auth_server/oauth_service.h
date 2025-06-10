#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TOAuthUserInfoResult
{
    std::string Subject;
    std::string Login;
};

////////////////////////////////////////////////////////////////////////////////

struct IOAuthService
    : public virtual TRefCounted
{
    virtual TFuture<TOAuthUserInfoResult> GetUserInfo(const std::string& accessToken) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOAuthService)

////////////////////////////////////////////////////////////////////////////////

IOAuthServicePtr CreateOAuthService(
    TOAuthServiceConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
