#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateCachingSecretVaultService(
    TCachingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TRegistry profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
