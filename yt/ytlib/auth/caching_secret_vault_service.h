#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateCachingSecretVaultService(
    TCachingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
