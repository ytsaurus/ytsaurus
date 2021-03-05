#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfigPtr config,
    ITvmServicePtr tvmService,
    NConcurrency::IPollerPtr poller,
    NProfiling::TRegistry profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
