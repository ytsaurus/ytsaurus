#pragma once

#include "public.h"

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicRetryableClientSpec
    : public virtual NYTree::TYsonStruct
{
    TDuration MinInnerTimeout; // Timeout for one request.
    TDuration Timeout;         // Global timeout for stopping retries.
    TExponentialBackoffOptions Backoff;

    REGISTER_YSON_STRUCT(TDynamicRetryableClientSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicRetryableClientSpec)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
