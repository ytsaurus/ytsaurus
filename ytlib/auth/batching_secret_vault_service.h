#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateBatchingSecretVaultService(
    TBatchingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
