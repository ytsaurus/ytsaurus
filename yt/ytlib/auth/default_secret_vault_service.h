#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfig config,
    ITvmServicePtr tvmService,
    NConcurrency::IPollerPtr poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
