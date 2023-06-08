#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TOAuthUserInfoResult {
    TString Sub;
    TString Login;
};

////////////////////////////////////////////////////////////////////////////////

struct IOAuthService
    : public virtual TRefCounted
{
    virtual TFuture<TOAuthUserInfoResult> GetUserInfo(const TString& accessToken) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOAuthService)

////////////////////////////////////////////////////////////////////////////////

IOAuthServicePtr CreateOAuthService(
    TOAuthServiceConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

}
