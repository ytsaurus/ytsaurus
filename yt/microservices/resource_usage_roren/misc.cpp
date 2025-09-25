#include "misc.h"

#include <util/system/env.h>
#include <yt/yt/library/auth/auth.h>

TString LoadResourceUsageToken()
{
    auto ytResourceUsageToken = GetEnv("YT_RESOURCE_USAGE_TOKEN");
    Y_ABORT_IF(ytResourceUsageToken.empty());
    NYT::NAuth::ValidateToken(ytResourceUsageToken);
    return ytResourceUsageToken;
}

