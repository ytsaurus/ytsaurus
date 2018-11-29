#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateCachingSecretVaultService(
    TCachingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
