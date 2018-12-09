#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfigPtr config,
    ITvmServicePtr tvmService,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
