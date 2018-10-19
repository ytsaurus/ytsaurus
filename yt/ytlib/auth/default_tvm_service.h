#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateDefaultTvmService(
    TDefaultTvmServiceConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
