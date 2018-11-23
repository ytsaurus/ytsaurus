#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfigPtr config,
    ITvmServicePtr tvmService,
    NConcurrency::IPollerPtr poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
