#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

IBlackboxServicePtr CreateDefaultBlackboxService(
    TDefaultBlackboxServiceConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
